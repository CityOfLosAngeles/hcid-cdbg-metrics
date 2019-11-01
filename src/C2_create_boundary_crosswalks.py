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

tracts = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/tracts_clipped.geojson')
zipcodes = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/zipcodes_clipped.geojson')
neighborhoods = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/neighborhoods.geojson')
neighborhood_councils = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/neighborhood_councils.geojson')
council_districts = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/council_districts.geojson')
congressional_districts = gpd.read_file('s3://hcid-cdbg-project-ita-data/gis/raw/congressional_districts_clipped.geojson')


# Subset and rename columns
tracts = tracts[['GEOID', 'clipped_area', 'geometry']]

zipcodes = zipcodes[['ZIPCODE', 'geometry']]
# Get rid of a problematic zipcode (Geometry Collection, not Polygon/MultiPolygon)
zipcodes = zipcodes[zipcodes.ZIPCODE != '91608']
zipcodes.rename(columns = {'ZIPCODE': 'ID'}, inplace = True)

neighborhoods = neighborhoods[['LATimesID', 'geometry']]
neighborhoods.rename(columns = {'LATimesID': 'ID'}, inplace = True)

neighborhood_councils = neighborhood_councils[['NC', 'geometry']]
neighborhood_councils.rename(columns = {'NC': 'ID'}, inplace = True)

council_districts = council_districts[['District', 'geometry']]
council_districts.rename(columns = {'District': 'ID'}, inplace = True)

congressional_districts = congressional_districts[['GEOID', 'geometry']]
congressional_districts.rename(columns = {'GEOID': 'ID'}, inplace = True)


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
    #if key.find('congressional') != -1:
        #gdf.rename(columns = {'GEOID_1': 'GEOID', 'GEOID_2': 'GEOID_congress'}, inplace = True)
    processed[key] = gdf


# Make the crosswalk wide (use to merge with Census tabular data)
wide_dfs = {}

for key, df in processed.items():
    # Convert ID to integer, so later we can take the max on it
    df['ID'] = df.ID.astype(int)
    # Count number of obs for each tract (how many CDs, NCs it overlaps with)
    df['obs'] = df.groupby('GEOID').cumcount() + 1
    df['max_val'] = df.groupby('GEOID')['obs'].transform('max')
    n = df.obs.max()
    # Loop over each observation and fill in the wide df
    for i in range(1, n + 1):
        id_col = f"ID{i}"
        allocate_col = f"allocate{i}"
        df[id_col] = df.apply(lambda row: row.ID if row.obs==i else np.nan, axis = 1)
        df[id_col] = df.groupby('GEOID')[id_col].transform('max')
        df[allocate_col] = df.apply(lambda row: row.allocate if row.obs==i else np.nan, axis = 1)
        df[allocate_col] = df.groupby('GEOID')[allocate_col].transform('max')
    df.drop(columns = ['ID', 'allocate', 'obs'], inplace = True)
    df = df.drop_duplicates()
    wide_dfs[key] = df


# Export the crosswalk as a parquet
for key, value in wide_dfs.items():
    value.sort_values('GEOID', ascending = True).to_parquet(f'./gis/crosswalk_tracts_{key}.parquet')
    value.sort_values('GEOID', ascending = True).to_parquet(f's3://hcid-cdbg-project-ita-data/gis/crosswalk_tracts_{key}.parquet')