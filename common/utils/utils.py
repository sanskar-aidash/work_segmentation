import math
import shutil
import os

from common.constants import FEEDER_COL, SPLIT_ID, AUTO_ID_COL
from common.constants import AUTO_ID_COL
from shapely.geometry import LineString
import pandas as pd
import geopandas as gpd
import pyproj

from shapely.geometry import shape

from shapely.ops import transform
from functools import partial


def change_crs(df):
    df_tr = df.copy()
    # print(f"crs = {df.crs}")
    if df.crs != None:
        # df_tr = df_tr.to_crs({"init":"epsg:4326"})
        df_tr = df_tr.to_crs("EPSG:4326")
    else:
        df_tr.crs = {"init": "epsg:4326", "no_defs": True}
    return df_tr


def transfrom_crs(df, to_crs):
    crs_str = f"EPSG:{to_crs}"
    return df.to_crs(crs_str)


def get_line_length(line):
    return line.length


def get_feeder_linemiles(df):
    df_mod = df.copy()
    df_mod = transfrom_crs(df_mod, to_crs="3857")
    df_mod["length"] = df_mod.apply(
        lambda row: get_line_length(row["geometry"]), axis=1
    )
    df_mod["length"] = df_mod["length"] * 0.000621371
    # feeder_lms = df_mod['length'].sum() * 0.000621371
    return df_mod


def get_linemiles(df1, df2, feeder_col):
    # df1 = df1.rename(columns = {feeder_col:FEEDER_COL})
    df1_mod = get_feeder_linemiles(df1)
    df2_mod = get_feeder_linemiles(df2)
    df1_grp = (
        df1_mod.groupby([FEEDER_COL])
        .agg(orig_spans=("length", "count"), orig_lms=("length", "sum"))
        .reset_index()
    )
    df2_grp = (
        df2_mod.groupby([FEEDER_COL, SPLIT_ID])
        .agg(exploded_spans=("length", "count"), exploded_lms=("length", "sum"))
        .reset_index()
    )
    comb_df = df1_grp.merge(df2_grp, on=[FEEDER_COL])
    df2_mod = change_crs(df2_mod)
    return comb_df, df2_mod


def get_line_list(geom):
    crd = geom.coords
    line_list = []
    for i in range(len(crd) - 1):
        line = LineString([crd[i], crd[i + 1]])
        line_list.append(line)
    return line_list


def explode_shp(orig_shp):
    print("----EXPLODING SHPS----")
    # Explode Multiline Linestring to LineString
    orig_shp = orig_shp.explode()
    # Linestring of multiple points to Linestring of two points
    orig_shp["lines"] = orig_shp.apply(
        lambda row: get_line_list(row["geometry"]), axis=1
    )
    orig_shp = orig_shp.drop("geometry", axis=1)
    orig_exp = pd.DataFrame(orig_shp).explode("lines").reset_index(drop=True)
    orig_exp = orig_exp.rename(columns={"lines": "geometry"})
    # Converting to GeoDataFrame
    gdf = gpd.GeoDataFrame(orig_exp)
    print(f"crs = {gdf.crs}")
    # print(gdf.head())
    # Changing CRS
    gdf = change_crs(gdf)
    print(f"crs = {gdf.crs}")
    # Filtering point-sized lines
    gdf["length"] = gdf.apply(lambda row: get_line_length(row["geometry"]), axis=1)
    gdf_filt = gdf[gdf["length"] > 1e-06]
    # print(gdf.head())
    return gdf




def area(geom):
    geom = {"type": "Polygon", "coordinates": geom}
    s = shape(geom)
    proj = partial(
        pyproj.transform, pyproj.Proj(init="epsg:4326"), pyproj.Proj(init="epsg:3857")
    )
    s_new = transform(proj, s)
    projected_area = transform(proj, s).area * 1e-6
    return projected_area


def convexHull(x):
    shp_file = x
    shp_file = shp_file[shp_file.geometry.notna()]
    shp_file = shp_file.to_crs("EPSG:4326")
    convex_hull = shp_file["geometry"].unary_union.convex_hull
    geo_convex = gpd.GeoSeries([convex_hull])
    geo_convex = geo_convex.scale(xfact=1.12, yfact=1.12, zfact=1, origin="center")
    # lat,_long = geo_convex[0].boundary.xy
    # lat,_long = convex_hull.boundary.xy
    # convex_hull = [[lat[i],_long[i]] for i in range(len(lat))]
    return geo_convex.geometry


def nearest_split_id(shp, current_split_id):
    wrong_split_shp = shp[shp[SPLIT_ID] == current_split_id]
    for _, span in wrong_split_shp.iterrows():
        row_point_1, row_point_2 = span["geometry"].boundary
        circle_buffer_1 = row_point_1.buffer(0.0036)
        circle_buffer_2 = row_point_2.buffer(0.0036)
        for _, f_row in shp.iterrows():
            if (
                f_row["geometry"].intersects(circle_buffer_1)
                or f_row["geometry"].intersects(circle_buffer_2)
                or f_row["geometry"].intersects(span["geometry"])
                and f_row[SPLIT_ID] != current_split_id
            ):
                return f_row[SPLIT_ID]


def get_split_shp(orig_shp, min_area):
    feeders_list = orig_shp[FEEDER_COL].unique()
    tiled_shp_list = []
    for feeder in feeders_list:
        print(f"feeder = {feeder}")
        shp = orig_shp[orig_shp[FEEDER_COL] == feeder].reset_index(drop=True)
        shp_tiled = tile_shp(shp, min_area)
        tiled_shp_list.append(shp_tiled)
    comb = pd.concat(tiled_shp_list, axis=0).reset_index(drop=True)
    return comb

def tile_shp(shp, min_area):
    print("----SPLITTING SHPS----")

    # c = fiona.open(shp_file_path)
    # geom = box(*c.bounds)

    min_len = math.sqrt(min_area * 5)
    max_tile_len = 0.02 / 1.5 * min_len
    # print(max_tile_len)

    x_start = shp.total_bounds[0]
    y_start = shp.total_bounds[1]
    x_end = shp.total_bounds[2]
    y_end = shp.total_bounds[3]

    tiles = gpd.GeoDataFrame()
    tile_num = 0

    for i in range(0, int((x_end - x_start) / max_tile_len + 1)):
        for j in range(0, int((y_end - y_start) / max_tile_len + 1)):
            tile_num += 1
            tmp_tile = (
                x_start + max_tile_len * (i),
                y_start + max_tile_len * (j),
                x_start + max_tile_len * (i + 1),
                y_start + max_tile_len * (j + 1),
            )
            tmp_tile_geom = box(*tmp_tile)
            tmp_df = gpd.GeoDataFrame(
                {"id": int(tile_num), "geometry": [tmp_tile_geom]}
            )
            tiles = tiles.append(tmp_df)

    #     tiles.to_file('boundary.shp')
    #     shp = gpd.read_file(shp_file_path)

    for index_1, box in tiles.iterrows():
        for index_2, span in shp.iterrows():
            if span.geometry.intersects(box.geometry):
                shp.loc[index_2, SPLIT_ID] = box.id

    split_ids = shp[SPLIT_ID].unique()
    for split in split_ids:
        tmp_shp = shp[shp[SPLIT_ID] == split]
        try:
            convex_hull = convexHull(tmp_shp)
            area_convex_hull = area(convex_hull)
            if area_convex_hull < min_area:
                new_split_ID = nearest_split_id(shp, split)
                shp.loc[shp[SPLIT_ID] == split, SPLIT_ID] = new_split_ID
        except:
            new_split_ID = nearest_split_id(shp, split)
            shp.loc[shp[SPLIT_ID] == split, SPLIT_ID] = new_split_ID

    shp[SPLIT_ID] = shp[SPLIT_ID].astype(int)
    return shp

def update_bit(shp, file_type):
    print(file_type)
    if file_type.lower() == "corrected":
        shp["correction_bit"] = 1
    elif file_type.lower() == "validated":
        shp["validation_bit"] = 1
    else:
        raise Exception("Invalid File Type Input")
    return shp


def get_insert_args(row_dict):
    insert_args = []
    for f in row_dict.keys():
        # Params For Insert
        if f == "geometry":
            insert_args.append(row_dict[f].wkt)
        else:
            insert_args.append(row_dict[f])
    return tuple(insert_args)


def get_field_args(row_dict, pk_fields, table, schema=None, total_rows=1):
    if schema:
        rel = "%s.%s" % (schema, table)
    else:
        rel = table
    print(f"rel:{rel}")

    fields, field_placeholders = [], []
    set_clause_list, set_args = [], []
    for f in row_dict.keys():
        fields.append(f)
        # Params For Insert
        if f == "geometry":
            field_placeholders.append("ST_GeomFromText(%s,4326)")
        else:
            field_placeholders.append("%s")

        # Params for Update
        if f not in pk_fields:
            set_clause_list.append(f'"{f}"=EXCLUDED."{f}"')

    set_clause_str = ", ".join(set_clause_list)
    fields_str = ", ".join('"{0}"'.format(f) for f in fields)
    pk_fields_str = ",".join('"{0}"'.format(f) for f in pk_fields)
    field_placeholder_str = ",".join(field_placeholders)
    field_placeholder_str = ",".join([f"({field_placeholder_str})"] * total_rows)

    fmt_args = (rel, fields_str, field_placeholder_str, pk_fields_str, set_clause_str)

    return fmt_args


def upsert_multiple_rows(
    db_cur, table_name, pk_fields, schema_name, df, on_conflict="update"
):
    assert len(pk_fields) > 0, "There must be at least one field as a primary key"

    # Forming Insert Query
    # fmt_args = get_field_args(df.iloc[0].to_dict(), pk_fields, table_name, schema = schema_name, total_rows = len(df))
    fmt_args = get_field_args(
        df.iloc[0].to_dict(), pk_fields, table_name, schema=schema_name, total_rows=1
    )
    # print(f"fmt_args={fmt_args}")
    # insert_query = "INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s" % fmt_args
    # print(insert_query)

    # Extracting Row Values To Insert
    insert_args_list = []
    for i, row in df.iterrows():
        # print("i=",i)
        insert_args = get_insert_args(row.to_dict())
        # print(f"insert_args={insert_args}")
        insert_args_list.append(insert_args)
    # print(f"insert_args list={insert_args_list}")

    # print('args str = ')
    args_str = b",".join(db_cur.mogrify(fmt_args[2], x) for x in insert_args_list)
    args_str = args_str.decode()
    # print(args_str)

    # print("---------INSERT QUERY --------")
    if on_conflict.lower() == "update":
        insert_query = f"INSERT INTO {fmt_args[0]} ({fmt_args[1]}) VALUES {args_str} ON CONFLICT ({fmt_args[3]}) DO UPDATE SET {fmt_args[4]}"
    else:
        insert_query = f"INSERT INTO {fmt_args[0]} ({fmt_args[1]}) VALUES {args_str} ON CONFLICT ({fmt_args[3]}) DO NOTHING"

    print(insert_query)
    # cur.execute("INSERT INTO table VALUES " + args_str)

    # Printing the exact query
    # args_str = db_cur.mogrify(insert_query, insert_args_list)

    # Executing Multiple Insert
    db_cur.execute(insert_query)
