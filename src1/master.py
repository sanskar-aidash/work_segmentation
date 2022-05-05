from lib2to3.pgen2.pgen import DFAState
import os
import sys
import geopandas as gpd
import pandas as pd
import multiprocessing 

from src1.attribute_wise_segmentation import *
from common.dba import client
from common.constants import *

def run_segmentation_1(customer_name, shape_file_type):

    # import ipdb; ipdb.set_trace()

    try:

        ########################################################################################################################
        # db = client.DBObject()
        # shape_file = gpd.GeoDataFrame.from_postgis(
        #     "SELECT * from %s.%s;"
        #     % (customer_name, shape_file_type),
        #     db.connection,
        #     geom_col="geometry",
        # )
        # print(shape_file.columns)

        path_object = "/Users/sanskar111100/Downloads/liberty_old_infra/liberty_old_infra.shp"
        shape_file = gpd.read_file(path_object)
        print(shape_file.columns)

        ########################################################################################################################
        shp_master = shape_file
        shp_master['auto_id_co'] = shp_master['feeder_wit']
        feeders = list(shp_master['feeder'].unique())

        os.system('mkdir results/')
        seg_func(shp_master, feeders)
        
        
        # final_output = "results/segments_" + customer_name + ".shp"

        ############################################ OUTPUT CSV CONVERT ########################################################

        # from common.output_converter import add_segmentation

        # csv_df = pd.read_sql_query(
        #     "SELECT * from %s.span_data;" % (customer_name),
        #     db.connection,
        # )
        # csv = "tmp/" + customer_name + "/final_output.csv"
        # csv_df.to_csv(csv)

        # final_segment_csv = add_segmentation(final_output, csv)
        # final_segment_shp = gpd.read_file(final_output)

        # final_segment_shp = final_segment_shp.rename(
        #     columns={"feeder_wit": "feeder_with_auto_id_corr",
        #              "feeder_w_1": "feeder_with_auto_id"}
        # )

        # print(final_segment_csv["seg_exp"].unique())
        # print(final_segment_shp["seg_exp"].unique())

        #################################### UPLOAD RESULT TO DB ###############################################################
        # print(final_segment_shp.columns)
        # print(len(final_segment_shp))
        # final_segment_shp = final_segment_shp[~final_segment_shp['feeder_with_autoid'].isna()]

        # db.upsert_many(customer_name, WORKSEGMENT_SHP_TABLE_NAME, final_segment_shp, PRIMARY_KEYS_ANALYSIS)

        # db.close()

        # db = client.DBObject()
        # result = db.update("UPDATE %s.project_status set work_segmentation_status=1;" % customer_name)
    except:
        pass

    """
Segmentation 1:
    1. customer_name - String
    2. shape_file_type - Drop down menu of list ["original_data", "corrected_data"]
    """