# Import boundary files, project, and clip to City Boundary
## HCID asked for: council districts, zipcodes, congressional districts, census tracts
## Might be useful: neighborhood councils, LA Times neighborhoods

import numpy as np
import pandas as pd
import geopandas as gpd
import intake
import os
import boto3
import utils
from datetime import datetime

time0 = datetime.now()
print(f'Start time: {time0}')

catalog = intake.open_catalog('./catalogs/*.yml')
s3 = boto3.client('s3')

bucket_name = 'hcid-cdbg-project-ita-data'
S3_FILE_PATH = f's3://{bucket_name}/'


# Open Data Catalog
city_boundary = catalog.city_boundary.read()
council_districts = catalog.council_districts.read()
neighborhoods = catalog.latimes_neighborhoods.read()
neighborhood_councils = catalog.neighborhood_councils.read()
zipcodes = catalog.zip_codes.read()

# Census TIGER shapefiles
tracts = gpd.read_file(f'{S3_FILE_PATH}gis/source/tl_2019_06_tract/')
block_groups = gpd.read_file(f'zip+{S3_FILE_PATH}gis/source/CENSUS_BLOCK_GROUPS_2010.zip')
congressional_districts = gpd.read_file(f'{S3_FILE_PATH}gis/source/tl_2019_us_cd116/')


# Clean up columns
city_boundary = city_boundary[['CITY', 'geometry']].to_crs('EPSG:2229')

council_districts = (council_districts[['NAME', 'District', 'District_Name', 'geometry']]
                     .rename(columns = {'NAME': 'CD'} )
                    )

neighborhoods.rename(columns = {'OBJECTID': 'LATimesID'}, inplace = True)

neighborhood_councils = (neighborhood_councils[['FID', 'Name', 'geometry']]
                         .rename(columns = {'FID': 'NC', 'Name': 'NC_Name'})
                        )

zipcodes = zipcodes[['ZIPCODE', 'geometry']]

tracts = tracts[['GEOID', 'NAMELSAD', 'geometry']]

block_groups = (block_groups[['GEOID10', 'COMMNAME', 'geometry']]
                .rename(columns = {'GEOID10': 'GEOID'})
               )

congressional_districts = congressional_districts[['GEOID', 'NAMELSAD', 'geometry']]


# For each boundary, do spatial join and clip using the City boundary

boundaries = {'council_districts': council_districts, 
              'neighborhoods': neighborhoods, 'neighborhood_councils': neighborhood_councils, 
              'zipcodes': zipcodes, 'tracts': tracts, 'block_groups': block_groups, 
              'congressional_districts': congressional_districts}


# Grab the city boundary's geometry
boundary = city_boundary.geometry.iloc[0]

gdfs = {}

for key, value in boundaries.items():
    # Project to CA State Plane
    value = value.to_crs('EPSG:2229')
    # Spatial join and only keep if it intersects with city_boundary
    joined = gpd.sjoin(value, city_boundary, how = 'inner', op = 'intersects')
    joined.drop(columns = ['index_right', 'CITY'], inplace = True)
    # Add area column
    joined['full_area'] = joined.geometry.area / utils.sqft_to_sqmi
    # Clip by city boundary and create new clipped geometry column
    joined['clipped_geom'] = joined[joined.intersects(boundary)].intersection(boundary)  
    # Add clipped_geom area column
    joined['clipped_area'] = joined.set_geometry('clipped_geom').area / utils.sqft_to_sqmi
    # Save result in new dictionary
    gdfs[key] = joined


# Save gdfs locally and to S3
if os.environ.get('DEV') is None:
    city_boundary.to_file(driver = 'GeoJSON', filename = f'./gis/raw/city_boundary.geojson')
    s3.upload_file(f'./gis/raw/city_boundary.geojson', bucket_name, 'gis/raw/city_boundary.geojson')


if os.environ.get('DEV') is None:
    for key, value in gdfs.items():
        if (key.find('tracts')  != -1) | (key.find('zipcodes') != -1) | (key.find('congressional') != -1) | (key.find('block') != -1):
            # Save a gdf with full area (GeoJSON can't handle multiple geometry columns)
            value.drop(columns = ['clipped_geom', 'clipped_area']).to_file(driver = 'GeoJSON', filename = f'./gis/raw/{key}_full.geojson')
            s3.upload_file(f'./gis/raw/{key}_full.geojson', bucket_name, f'gis/raw/{key}_full.geojson')
            # Save a gdf with clipped area
            value.drop(columns = ['geometry', 'full_area']).set_geometry('clipped_geom').to_file(driver = 'GeoJSON', filename = f'./gis/raw/{key}_clipped.geojson')
            s3.upload_file(f'./gis/raw/{key}_clipped.geojson', bucket_name, f'gis/raw/{key}_clipped.geojson')
        else:
            # Save the full area (everything falls within city boundary)
            value.drop(columns = ['clipped_geom', 'clipped_area']).to_file(driver = 'GeoJSON', filename = f'./gis/raw/{key}.geojson')
            s3.upload_file(f'./gis/raw/{key}.geojson', bucket_name, f'gis/raw/{key}.geojson')


time1 = datetime.now()
print(f'Total execution time: {time1 - time0}')   