import sys
import json
from os import path
import inspect
import logging
import logging.config
import numpy as np
import argparse
import time

from src1.master import run_segmentation_1
from src2.master import run_segmentation_2
from src3.master import run_segmentation_3

"""
@infra_service_entrypoint_wrapper
def trigger_docker(**kwargs):
    Trigger the code inside docker

    Returns:

    
    for handler in logging.root.handlers:
        handler.addFilter(ContextFilter(kwargs["job_id"]))

    # logger.info(f"Received message: {record}")
    logger.info(f"Received Massage {kwargs}")

    start_time = time.time()
    logger.info(f"received trigger: {kwargs}")
    seg_type = kwargs["seg_type"]
    customer_name = kwargs["customer"]
    shape_file_type = kwargs["shapefile_type"]
    

    if seg_type==1:
        return run_segmentation_1(customer_name, shape_file_type)
    elif seg_type==2:
        lower_limit = float(kwargs["lower_limit"])
        upper_limit = float(kwargs["upper_limit"])
        feeder_name = kwargs["feeder_name"]
        return run_segmentation_2(customer_name, shape_file_type, lower_limit, upper_limit, feeder_name)
    elif seg_type==3:
        device_file_list = kwargs["device_shp_list"]
        s3_bucket = kwargs["s3_bucket"]
        devices_to_map = kwargs["devices_to_map"]
        bounds_min = float(kwargs["bounds_min"])
        bounds_max = float(kwargs["bounds_max"])
        radius_1 = float(kwargs["radius_1"])
        radius_2 = float(kwargs["radius_2"])
        radius_3 = float(kwargs["radius_3"])
        return run_segmentation_3(customer_name, shape_file_type, device_file_list, s3_bucket, devices_to_map, bounds_min, bounds_max, radius_1, radius_2, radius_3)

    # logger.info('{} Work segmentation successful!'.format("work segmentation: "))
    # return {
    #     'status': 'SUCCESS',
    #     'time_taken': f"{(time.time() - start_time)}s",
    #     'output': {
    #         'errors': []
    #     }
    # }
"""



if __name__ == "__main__":
    customer_name = "greystone"
    print("Got customer name")
    shape_file_type = "original_data"
    run_segmentation_1(customer_name, shape_file_type)