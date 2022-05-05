import geopandas as gpd
import ast


def read_devices(device_shape_list):
    device_shape_list = ast.literal_eval(device_shape_list)

    devices = list()
    if device_shape_list:
        for device_path in device_shape_list:
            device_gdf = gpd.read_file(device_path)
            device_gdf = device_gdf.to_crs('epsg:4326')
            devices.append(device_gdf)

    return devices


def map_devices(span_shape, devices_list, devices_to_map):

    span_shape = span_shape.to_crs('epsg:4326')
    span_shape = span_shape.reset_index()
    span_shape['has_device'] = [0] * len(span_shape)

    if len(devices_to_map) == 0:
        devices_to_map = []
        for devices in devices_list:
            devices_to_map += list(devices.OBJECTID)

    for devices in devices_list:
        for index, device in devices.iterrows():
            if device.OBJECTID in devices_to_map:
                buffer_ = device.geometry.buffer(0.00072/2)
                for i, span in span_shape.iterrows():
                    if span.geometry is None:
                        span_shape.drop(i, inplace=True)
                        continue

                    if buffer_.intersects(span.geometry):
                        span_shape.loc[i, 'has_device'] = 1
                        break

        return span_shape