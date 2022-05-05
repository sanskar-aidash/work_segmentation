from lib2to3.pgen2.pgen import DFAState
import os
import sys
import geopandas as gpd
import pandas as pd

from src2.len_wise_segmentation import *
from common.dba import client
from common.constants import *
#from common.dba.client import DBObject

def run_segmentation_2(customer_name, shape_file_type, lower_limit, upper_limit, feeder_name=None):

    # import ipdb; ipdb.set_trace()

    try:

        ########################################################################################################################
        db = client.DBObject()
        shape_file = gpd.GeoDataFrame.from_postgis(
            "SELECT * from %s.%s;"
            % (customer_name, shape_file_type),
            db.connection,
            geom_col="geometry",
        )
        print(shape_file.columns)

        ########################################################################################################################
        companyName = customer_name
        path = shape_file
        feederCol = 'feeder'
        feederName = feeder_name
        spanIdCol = 'feeder_with_autoid'
        lowerLimit = lower_limit
        upperLimit = upper_limit
        
        final_output = " "
        os.system('mkdir results/')
        if feederName==None:
            final_segment_shp = run_the_segmentation(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit, companyName)
            final_output = "results/" + companyName + ".shp"
        else:
            final_segment_shp = run_the_segmentation_on_feeder(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit, companyName)
            final_output = "results/" + companyName + "_" + feederName + ".shp"

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
        
        import ipdb; ipdb.set_trace()
        
        print(final_segment_shp.columns)
        print(len(final_segment_shp))
        final_segment_shp = final_segment_shp[~final_segment_shp['feeder_with_autoid'].isna()]

        db.upsert_many(customer_name, WORKSEGMENT_SHP_TABLE_NAME, final_segment_shp, PRIMARY_KEYS_ANALYSIS)

        db.close()

        db = client.DBObject()
        result = db.update("UPDATE %s.project_status set work_segmentation_status=1;" % customer_name)
    except:
        pass

    """
Segmentation 2:
    1. customer_name - String
    2. shape_file_type - Drop down menu of list ["original_data", "corrected_data"]
    3. lower_limit - Float
    4. upper_limit - Float
    5. feeder_name - String (Optional)
    """