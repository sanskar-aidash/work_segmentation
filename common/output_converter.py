import geopandas as gpd
import pandas as pd


def add_segmentation(shp, csv):

    shp_df = gpd.read_file(shp)
    csv_df = pd.read_csv(csv)

    filtered_shp_df = shp_df.filter(["ID", "seg_exp", "segment_tr"])

    merged_df = csv_df.merge(filtered_shp_df, on=["ID"])
    return merged_df