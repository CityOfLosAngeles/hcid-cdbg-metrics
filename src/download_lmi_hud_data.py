import geopandas

CHUNK = 1000
offset = 0

lmi_data = geopandas.GeoDataFrame()
while True:
    url = (
        "https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services/"
        "LMISD2015forService/FeatureServer/0/query?"
        "where=State%3D06+AND+County%3D037&objectIds=&time=&geometry="
        "&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects"
        "&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false"
        "&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault"
        "&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR="
        "&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false"
        "&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false"
        "&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false"
        "&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having="
        f"&resultOffset={offset}&resultRecordCount=&returnZ=false&returnM=false"
        "&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none"
        "&f=pgeojson&token="
    )
    gdf = geopandas.read_file(url, driver="GeoJSON")
    if len(gdf):
        lmi_data = lmi_data.append(gdf)
        offset = offset + CHUNK
    else:
        break
        
S3_FILE_PATH = "s3://hcid-cdbg-project-ita-data/lmi_hud.geojson"
lmi_data.to_file(S3_FILE_PATH, driver="GeoJSON")
