import os
import geopandas as gpd


def merge_segmented_shp(directory, out_path):

    paths = [x[0] for x in os.walk(directory)]
    shp_paths = []

    for path in paths:
        for file in os.listdir(path):
            if file.endswith(".shp"):
                # print(file)
                shp_paths.append(os.path.join(path, file).replace("\\", "/"))

    shape = gpd.GeoDataFrame()

    for path in shp_paths:
        shp = gpd.read_file(path)
        shape = shape.append(shp)

    shape = shape.set_crs("epsg:4326")
    shape.to_file(out_path)