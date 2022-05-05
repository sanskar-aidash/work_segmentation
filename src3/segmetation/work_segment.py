import os
from itertools import product

import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
from shapely.geometry import mapping, Polygon
import fiona
from multiprocessing import Pool
from functools import partial


################################################################################################################################################################################
def adjacent_line_str(gdf_row, shp, seg, linemiles, seg_change, seg_flag, bounds_max, bounds_min, radius_1):
    adjacent_df = pd.DataFrame([])
    row_point_1, row_point_2 = gdf_row['geometry'].boundary
    distance = float(radius_1)
    circle_buffer_1 = row_point_1.buffer(distance)
    circle_buffer_2 = row_point_2.buffer(distance)
    
    for index, f_row in shp.iterrows():
        try:
            endpoint_1, endpoint_2 = f_row['geometry'].boundary
            if f_row['geometry'].intersects(circle_buffer_1) or f_row['geometry'].intersects(circle_buffer_2) or f_row[
                'geometry'].intersects(gdf_row['geometry']):
                if f_row['BFS_flag'] == 0:
                    adjacent_df = adjacent_df.append(f_row)
                    shp.loc[index, 'BFS_flag'] = 1
                    linemiles = f_row['Length(mi)'] + linemiles
                    if f_row['has_device'] == 1.0 and linemiles > bounds_max:  # 5
                        print(seg)
                        seg = seg + 1
                        linemiles = 0

                        seg_change.append(index)
                        seg_flag.append(seg)
                    shp.loc[index, 'seg_exp'] = seg
        except:
            continue;
    return adjacent_df, seg, linemiles, seg_change, seg_flag;


def BFS_v2(line, shp, seg_id, lm, seg_change, seg_flag, bounds_max, bounds_min, radius_1):
    span_queue = []
    children, seg_id_ret, lm, seg_change, seg_flag = adjacent_line_str(line, shp, seg_id, lm, seg_change, seg_flag,
                                                                       bounds_max, bounds_min, radius_1)
    for index, row in children.iterrows():
        span_queue.append(index)
    while len(span_queue) > 0:
        span_tmp = span_queue.pop(0)  # DFS
        children, seg_id_ret, lm, seg_change, seg_flag = adjacent_line_str(shp.loc[span_tmp], shp, seg_id_ret, lm,
                                                                           seg_change, seg_flag, bounds_max, bounds_min,
                                                                           radius_1)
        for index, row in children.iterrows():
            span_queue.append(index)
    return seg_change, seg_flag, seg_id_ret;


def run_1st_module_MASTER(shp, tmp_res_1, bounds_max, bounds_min, radius_1):
    shp['BFS_flag'] = 0
    shp['rectification_flag'] = 0
    shp['seg_exp'] = 1
    seg_exp = 1
    lm = 0
    seg_change = []
    seg_flag = []
    while len(shp[shp['BFS_flag'] == 0]) > 0:
        try:
            ret_index_list, ret_seg_list, seg_exp = BFS_v2(shp[shp['BFS_flag'] == 0].iloc[0], shp, seg_exp, lm,
                                                           seg_change, seg_flag, bounds_max, bounds_min, radius_1)
            seg_exp += 1
        except:
            continue

    # shp=shp[shp['BFS_flag']==1]
    shp.to_file(tmp_res_1)


################################################################################################################################################################################
def adjacent_line_str_rectify(gdf_row, tmp_shp, seg, seg_new, radius_2):
    adjacent_df = pd.DataFrame([])
    row_point_1, row_point_2 = gdf_row['geometry'].boundary
    distance = float(radius_2)
    circle_buffer_1 = row_point_1.buffer(distance)
    circle_buffer_2 = row_point_2.buffer(distance)
    for index, f_row in tmp_shp.iterrows():
        endpoint_1, endpoint_2 = f_row['geometry'].boundary
        if f_row['geometry'].intersects(circle_buffer_1) or f_row['geometry'].intersects(circle_buffer_2) or f_row[
            'geometry'].intersects(gdf_row['geometry']):
            if f_row['rectify'] == 0:
                adjacent_df = adjacent_df.append(f_row)
                tmp_shp.loc[index, 'rectify'] = 1
                tmp_shp.loc[index, 'seg_exp'] = seg_new
            # if f_row['rectify'] == 0 and f_row['seg_exp']!=seg:
    return adjacent_df, seg


def BFS_v2_rectify(line, tmp_shp, seg_id, seg_new, radius_2):
    span_queue = []
    children, seg_id = adjacent_line_str_rectify(line, tmp_shp, seg_id, seg_new, radius_2)
    for index, row in children.iterrows():
        span_queue.append(index)
    while len(span_queue) > 0:
        span_tmp = span_queue.pop(0)  # DFS
        children, seg_id = adjacent_line_str_rectify(tmp_shp.loc[span_tmp], tmp_shp, seg_id, seg_new, radius_2)
        for index, row in children.iterrows():
            span_queue.append(index)
    return tmp_shp[tmp_shp['rectify'] == 1]  # seg_change, seg_flag, seg_id_ret;


def run_2nd_module_MASTER(tmp_res_1, tmp_res_2, radius_2):
    shp = gpd.read_file(tmp_res_1)
    # shp = shp.to_crs("epsg:4326")
    segs = list(shp['seg_exp'].unique())
    final = gpd.GeoDataFrame()
    shp['rectify'] = 0
    seg_new = sorted(segs)[-1] + 1
    for seg in segs:
        tmp_shp = shp[shp['seg_exp'] == seg]
        while len(tmp_shp[tmp_shp['rectify'] == 0]) > 0:
            tmp_shp = tmp_shp[tmp_shp['rectify'] == 0]
            tmp_final = BFS_v2_rectify(tmp_shp.iloc[0], tmp_shp, seg, seg_new, radius_2)
            final = final.append(tmp_final)
            seg_new = seg_new + 1
    final = final.set_crs("epsg:4326")
    final.to_file(tmp_res_2)


################################################################################################################################################################################
def adjacent_line_str_BFS_rectification(gdf_row, shp, seg, radius_3):
    adjacent_df = pd.DataFrame([])
    row_point_1, row_point_2 = gdf_row['geometry'].boundary
    distance = float(radius_3)
    circle_buffer_1 = row_point_1.buffer(distance)
    circle_buffer_2 = row_point_2.buffer(distance)
    for index, f_row in shp.iterrows():
        endpoint_1, endpoint_2 = f_row['geometry'].boundary
        if f_row['geometry'].intersects(circle_buffer_1) or f_row['geometry'].intersects(circle_buffer_2) or f_row[
            'geometry'].intersects(gdf_row['geometry']):
            if f_row['rectification_flag'] == 0:
                adjacent_df = adjacent_df.append(f_row)
                shp['rectification_flag'][index] = 1
    return adjacent_df, seg


def BFS_rectification_v2(line, shp, seg_id, segment_incorrc, radius_3):
    span_queue = []
    adjacent, seg_id_ret = adjacent_line_str_BFS_rectification(line, shp, seg_id, radius_3)
    for index, row in adjacent.iterrows():
        span_queue.append(index)
    while len(span_queue) > 0:

        span_tmp = span_queue.pop(0)  # DFS
        adjacent, seg_id_ret = adjacent_line_str_BFS_rectification(shp.loc[span_tmp], shp, seg_id, radius_3)
        if shp.loc[span_tmp]['seg_exp'] not in segment_incorrc:
            ret_cor = int(shp.loc[span_tmp]['seg_exp'])
            shp.loc[shp['seg_exp'] == seg_id, 'seg_exp'] = ret_cor
            print(seg_id, '----->', ret_cor)
            break;
        for index, row in adjacent.iterrows():
            span_queue.append(index)
            # return seg_change, seg_flag, seg_id_ret;


def Sort_Tuple(tup):
    tup.sort(key=lambda x: x[1], reverse=True)
    return tup


def run_3rd_module_MASTER(tmp_res_2, segmented, bounds_min, radius_3):
    shp = gpd.read_file(tmp_res_2)

    for iter in range(0, 4):
        segments = np.unique(list(shp['seg_exp']))
        segment_incorrc = []

        for segment in segments:
            s_tmp = shp[shp['seg_exp'] == segment]
            lm_sum = s_tmp['Length(mi)'].sum()
            if lm_sum < bounds_min:  # 2.5
                segment_incorrc.append((segment, lm_sum))
        segment_incorrc = Sort_Tuple(segment_incorrc)
        segment_incorrc = [aa[0] for aa in segment_incorrc]
        shp['rectification_flag'] = 0
        print(segment_incorrc)

        for s_id in segment_incorrc:
            print(s_id)
            seg_incorrc_df = shp[shp['seg_exp'] == s_id]
            seg_incorrc = seg_incorrc_df.iloc[0]
            # BFS_rectification(seg_incorrc, shp, s_id, segment_incorrc)
            BFS_rectification_v2(seg_incorrc, shp, s_id, segment_incorrc, radius_3)
            # shp['seg_exp'] = shp['seg_exp'].replace(regex=s_id, value=correc_id)

    shp.to_file(segmented)


################################################################################################################################################################################
def merge_device_spans(gdf_device_spans, master_gdf_path, out_path, buffer_=0.000072):
    master_gdf = gpd.read_file(master_gdf_path)
    gdf_device_spans['span_id'] = range(0, len(gdf_device_spans))
    gdf_device_spans_ = gdf_device_spans.copy()
    gdf_device_spans_.geometry = gdf_device_spans_.geometry.buffer(buffer_)
    gdf_device_spans_ = gpd.sjoin(gdf_device_spans_, master_gdf[['geometry', 'seg_exp']])
    gdf_device_spans_ = gdf_device_spans_.drop_duplicates(['span_id'])
    gdf_device_spans_seg = pd.merge(gdf_device_spans, gdf_device_spans_[['span_id', 'seg_exp']],
                                    on=['span_id'], how='left')

    final_gdf = pd.concat([master_gdf, gdf_device_spans_seg])

    final_gdf.to_file(out_path)


def segment_work(span_shp, output_path, bounds_max, bounds_min, radius_1, radius_2, radius_3, feeder_name):
    input_shp = span_shp[span_shp['feeder'] == feeder_name]
    input_shp["Length(mi)"] = input_shp["length_span"]
    input_shp_no_device = input_shp[~((input_shp['has_device'] == 1) & (input_shp['Length(mi)'] > 0.2 / 1.6 / 1000))]
    input_shp_devices = input_shp[(input_shp['has_device'] == 1) & (input_shp['Length(mi)'] > 0.2 / 1.6 / 1000)]

    tmp_res_1 = f"{output_path}/{feeder_name}_tmp_1.shp"
    run_1st_module_MASTER(input_shp_no_device, tmp_res_1, bounds_max, bounds_min, radius_1)
    print('First Module Done')

    tmp_res_2 = f"{output_path}/{feeder_name}_tmp_2.shp"
    run_2nd_module_MASTER(tmp_res_1, tmp_res_2, radius_2)
    print('Second Module Done')

    segmented = f"{output_path}/res/{feeder_name}"
    run_3rd_module_MASTER(tmp_res_2, segmented, bounds_min, radius_3)
    print('Third Module Done')

    segmented_pro = f"{output_path}/res_pro/{feeder_name}"
    merge_device_spans(input_shp_devices, segmented + f'/{feeder_name}.shp', segmented_pro, buffer_=0.000072)


def work_segmentation(inp_shp, out_path, feeders,
                      bounds_minimum=0,
                      bounds_maximum=0,
                      radius1=0.000072,
                      radius2=0.000072,
                      radius3=0.000072):

    pool = Pool(processes=6)
    func = partial(segment_work, inp_shp, out_path, float(bounds_minimum), float(bounds_maximum),
                   float(radius1), float(radius2), float(radius3))
    pool.map(func, feeders)