# from common.utils.secrets import get_secret_manger_values


# db_creds = get_secret_manger_values("labelling_db_credentials_tree_height")

# HOST = db_creds["HOST"]
# PORT = db_creds["PORT"]
# USERNAME = db_creds["USER"]
# PASSWORD = db_creds["PASSWORD"]
# DATABASE = db_creds["DATABASE"]


BUCKET_NAME = "satellite-test-data"

# SCHEMA_NAME = 'entergy'
# ORIGINAL_TABLE_NAME = 'analysis_dan'
# UPDATED_TABLE_NAME = 'analysis_dan_updated'
# FEASIBILITY_TABLE_NAME = 'data_status'
# BBOX_TABLE_NAME = 'custom_bbox'


# SCHEMA_NAME = 'excelon_test'            # Update
# CUSTOMER_NAME = 'excelon'               # Update
# FEEDER_NAME = 'feeder123'
# FEEDER_COL = 'FeederID'                 # Update

# Standardized Table Names
ORIGINAL_TABLE_NAME = "original_data"
UPDATED_TABLE_NAME = "corrected_data"
FEASIBILITY_TABLE_NAME = "data_status"
BBOX_TABLE_NAME = "custom_bbox"
ANALYSIS_TABLE_NAME = "analysis"
SPAN_DATA_TABLE_NAME = "span_data"
WORKSEGMENT_SHP_TABLE_NAME = "analysis_updated_table"
WORKSEGMENT_CSV_TABLE_NAME = "final_output_work_segments"


# Save Directory
ORIG_SHP_PATH = (
    "/home/ubuntu/mount/external_drive_4/dan_codes/data-acquisition/data/new/orig.zip"
)
INPUT_FILE_PATH = (
    "/home/ubuntu/mount/external_drive_4/dan_codes/data-acquisition/data/new/"
)
INPUT_FILE_NAME = "orig.zip"

# Standarized Column Names
AUTO_ID_COL = "auto_id"
SPLIT_ID = "split_id"
FEEDER_COL = "feeder"
CUSTOMER_COL = "customer"
PRIMARY_KEYS = ["feeder_with_autoid"]
PRIMARY_KEYS_BBOX = ["comb_id"]
PRIMARY_KEYS_FEASIBILITY = ["ID"]
PRIMARY_KEYS_ANALYSIS = ["feeder_with_autoid"]


# Model Constants
REGION_TYPE = "D"
RESOLUTION = 1.64
BBOX_CONFIG = "0#4#10#15"
FILTERED_SPANS = []

EXPIRATION_DURATION = 3600 * 24
MIN_AREA = 0.2