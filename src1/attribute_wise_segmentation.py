import pandas as pd 
import sys
import geopandas as gpd 
import warnings
warnings.filterwarnings('ignore')
import multiprocessing 
import os

shp_master = 0

def adjacent_homoginious_span(current_span , shp , radii , constraint1 , constraint2 ):
    
    # import ipdb; ipdb.set_trace()

    #print(current_span['geometry'])
    row_point_1, row_point_2 = current_span['geometry'].iloc[0].boundary
    circle_buffer_1 = row_point_1.buffer(radii)
    circle_buffer_2 = row_point_2.buffer(radii)
    adjancent_auto_id_corrs = []
    for index,row in shp[shp['visited']==0].iterrows():
        if row.geometry.intersects(circle_buffer_1) or row.geometry.intersects(circle_buffer_2):
            if (row[constraint1] == current_span[constraint1].iloc[0]) and (row[constraint2] == current_span[constraint2].iloc[0]) :
                #print(row.auto_id_co)
                adjancent_auto_id_corrs.append(row.auto_id_co)
    return adjancent_auto_id_corrs


def homogenious_segmentation(feeder_name):

    # import ipdb; ipdb.set_trace()

    # radii = 0.000072
    print("At line 33")
    shp = shp_master[shp_master['feeder'] == feeder_name]
    shp['visited'] = 0

    

    seg = 1
    while (len(shp[shp['visited']==0]) != 0):
        seg+=1
        print('SEG--->' , seg)
        not_visited = list(shp[shp['visited']==0]['auto_id_co'])
        current_auto_id_corr = not_visited[0]
        traversal_stack = [current_auto_id_corr]
        while len(traversal_stack)>0:
            current_auto_id_corr = traversal_stack.pop(0)
            current_span = shp[shp['auto_id_co'] == current_auto_id_corr]
            if shp[shp['auto_id_co'] ==current_auto_id_corr]['visited'].iloc[0] == 1:
                continue;
            else:
                shp.loc[shp['auto_id_co'] ==current_auto_id_corr , 'visited' ] = 1
            shp.loc[shp['auto_id_co'] ==current_auto_id_corr , 'SEG' ] = seg
            #print(current_auto_id_corr)
            adjacent_spans = adjacent_homoginious_span(current_span , shp , 0.000072 , 'equiptype' , 'right_of_w')
            traversal_stack = traversal_stack + adjacent_spans
    
    
    shp.to_file('results/segments_' + str(feeder_name))

def seg_func(shpMaster, feeders):
    
    # import ipdb; ipdb.set_trace()

    global shp_master
    shp_master = shpMaster
    pool = multiprocessing.Pool(30)
    print("Code was here!")
    pool.map(homogenious_segmentation, feeders)
    print("seg_func Done!")






