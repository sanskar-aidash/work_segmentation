import os
import sys
import geopandas as gpd
import pandas as pd

from common.dba import client
from common.constants import *
from src3.preprocessing.map_devices import *
from src3.segmetation.work_segment import work_segmentation
from src3.postprocessing.merge_shp import merge_segmented_shp


def run_segmentation_3(customer_name, shape_file_type, device_file_list, s3_bucket, devices_to_map, bounds_min, bounds_max, radius_1, radius_2, radius_3):
#s3_bucket = customer-raster-prod
    try:

        ########################################################################################################################
        db = client.DBObject()
        shape_file = gpd.GeoDataFrame.from_postgis(
            "SELECT * from %s.%s';"
            % (customer_name, shape_file_type),
            db.connection,
            geom_col="geometry",
        )
        print(shape_file.columns)

        ########################################################################################################################
        if device_file_list:
            device_list = read_devices(s3_bucket, device_file_list)
            device_mapped_span_gdf = map_devices(shape_file, device_list, devices_to_map)

        feeders = device_mapped_span_gdf.feeder.unique().tolist()
        print('devices_mapped', len(device_mapped_span_gdf[device_mapped_span_gdf['has_device'] == 1]))

        segment_output_path = "tmp/" + customer_name + '/tmp'
        os.mkdir("tmp")
        os.mkdir("tmp/" + customer_name)
        os.mkdir(segment_output_path)
        os.mkdir(segment_output_path + '/res')
        os.mkdir(segment_output_path + '/res_pro')

        work_segmentation(device_mapped_span_gdf, segment_output_path, feeders, bounds_min, bounds_max, radius_1, radius_2,
                          radius_3)
        print("Segmentation Done")

        final_output = "tmp/" + customer_name + "/" + customer_name + "_seg_res"
        merge_segmented_shp(f"{segment_output_path}/res_pro", final_output)
        print("Final Segmentation Done")

        ############################################ OUTPUT CSV CONVERT ########################################################

        from common.output_converter import add_segmentation

        csv_df = pd.read_sql_query(
            "SELECT * from %s.span_data;" % (customer_name),
            db.connection,
        )
        csv = "tmp/" + customer_name + "/final_output.csv"
        csv_df.to_csv(csv)

        final_segment_csv = add_segmentation(final_output, csv)
        final_segment_shp = gpd.read_file(final_output)

        final_segment_shp = final_segment_shp.rename(
            columns={"feeder_wit": "feeder_with_auto_id_corr",
                     "feeder_w_1": "feeder_with_auto_id"}
        )

        print(final_segment_csv["seg_exp"].unique())
        print(final_segment_shp["seg_exp"].unique())

        #################################### UPLOAD RESULT TO DB ###############################################################
        print(final_segment_shp.columns)
        print(len(final_segment_shp))
        final_segment_shp = final_segment_shp[~final_segment_shp['feeder_with_auto_id_corr'].isna()]

        db.upsert_many(customer_name, WORKSEGMENT_SHP_TABLE_NAME, final_segment_shp, PRIMARY_KEYS_ANALYSIS)

        db.close()

        db = client.DBObject()
        result = db.update("UPDATE %s.project_status set work_segmentation_status=1;" % customer_name)
    except:
        pass

    """
Segmentation 3:
    customer_name = kwargs["customer"]
    shape_file_type = kwargs["shapefile_type"]
    device_file_list = kwargs["device_shp_list"]
    s3_bucket = kwargs["s3_bucket"]
    devices_to_map = kwargs["devices_to_map"]
    bounds_min = float(kwargs["bounds_min"])
    bounds_max = float(kwargs["bounds_max"])
    radius_1 = float(kwargs["radius_1"])
    radius_2 = float(kwargs["radius_2"])
    radius_3 = float(kwargs["radius_3"])
    """