# Import boundary files, project, and clip to City Boundary
## HCID asked for: council districts, zipcodes, congressional districts, census tracts
## Might be useful: neighborhood councils, LA Times neighborhoods

import numpy as np
import pandas as pd
import geopandas as gpd
import intake
import os
import utils

catalog = intake.open_catalog('./catalogs/*.yml')

tracts = gpd.read_file('./gis/raw/tracts_clipped.geojson')
zipcodes = gpd.read_file('./gis/raw/zipcodes_clipped.geojson')
neighborhoods = gpd.read_file('./gis/raw/neighborhoods.geojson')
neighborhood_councils = gpd.read_file('./gis/raw/neighborhood_councils.geojson')
council_districts = gpd.read_file('./gis/raw/council_districts.geojson')
congressional_districts = gpd.read_file('./gis/raw/congressional_districts_clipped.geojson')


# Subset and rename columns
tracts = tracts[['GEOID', 'clipped_area', 'geometry']]

zipcodes = zipcodes[['ZIPCODE', 'geometry']]
# Get rid of a problematic zipcode (Geometry Collection, not Polygon/MultiPolygon)
zipcodes = zipcodes[zipcodes.ZIPCODE != '91608']

neighborhoods = neighborhoods[['LATimesID', 'geometry']]
neighborhood_councils = neighborhood_councils[['NC', 'geometry']]
council_districts = council_districts[['District', 'geometry']]
congressional_districts = congressional_districts[['GEOID', 'geometry']]


# Loop through each boundary, find the intersection between census tract and boundary
# Use area to allocate portion of census tract if it falls into multiple CDs, neighborhoods, etc
boundaries = {'zipcodes': zipcodes, 'neighborhoods': neighborhoods, 'neighborhood_councils': neighborhood_councils, 
              'council_districts': council_districts, 'congressional_districts': congressional_districts}

processed = {}

for key, value in boundaries.items():
    # Find the intersection of tracts and the larger boundary
    gdf = gpd.overlay(tracts, value, how = 'intersection')
    gdf['new_area'] = gdf.geometry.area / utils.sqft_to_sqmi
    gdf['intersect'] = gdf.new_area / gdf.clipped_area
    # For now, say that if 90% or more of the tract falls into the larger boundary, assign the entire tract to that boundary
    gdf = gdf[gdf.intersect >= 0.1]
    gdf['allocate'] = gdf.apply(lambda row: 1 if row.intersect >= 0.9 else row.intersect, axis = 1)
    gdf.drop(columns = ['clipped_area', 'new_area', 'intersect', 'geometry'] , inplace = True)
    if key.find('congressional') != -1:
        gdf.rename(columns = {'GEOID_1': 'GEOID', 'GEOID_2': 'GEOID_congress'}, inplace = True)
    processed[key] = gdf


# Export the crosswalk as a parquet
for key, value in processed.items():
    value.sort_values(['GEOID', 'allocate'], ascending = [True, False]).to_parquet(f'./gis/crosswalk_tracts_{key}.parquet')
    value.sort_values(['GEOID', 'allocate'], ascending = [True, False]).to_parquet(f's3://hcid-cdbg-project-ita-data/gis/crosswalk_tracts_{key}.parquet')