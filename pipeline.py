from prefect import task, Flow
import pandas as pd
import os
import xarray as xr
from prefect.executors import DaskExecutor
from pangeo_forge_recipes.storage import FSSpecTarget
from fsspec.implementations.local import LocalFileSystem
import shutil
import fsspec

from config import Config
from models import get_available_stations_from_cehq, StationParserCEHQ


def prepare_id_metadata(station):
    """
    Prepare id metadata

    """
    metadata_names = ["id", "name", "province", "longitude", "latitude",
                      "drainage_area", "regulated"]
    
    ds = pd.DataFrame(index=metadata_names,
                        data=[getattr(station, attr)
                              for attr in metadata_names]).T.set_index('id').to_xarray()
    
    ds['source'] = ("source", [station.source])
    ds['timestep'] = ("timestep", [station.timestep])
    ds['time_agg'] = ("time_agg", [station.time_agg])
    ds['spatial_agg'] = ("spatial_agg", [station.spatial_agg])
    
    ds['drainage_area'].attrs = {
        "long_name": "drainage_area",
        "units": "km2",
        }

    ds['latitude'].attrs = {
        "standard_name": "latitude",
        "long_name": "latitude",
        "units": "decimal_degrees",
            }
    ds['longitude'].attrs = {
        "standard_name": "longitude",
        "long_name": "longitude",
        "units": "decimal_degrees",
        }
    
    metadata_names.append('source')
    
    ds = ds.set_coords(metadata_names)

    return ds 


def prepare_values(station):
        
    ds_time = pd.DataFrame(columns=['time'],
             data=pd.date_range('1860-01-01', pd.Timestamp.now())).set_index('time').to_xarray()

    ds_values = station.values.to_xarray().rename({'value': station.data_type})
    ds_values[station.data_type] = ds_values[station.data_type].expand_dims(['id', 'spatial_agg', 'timestep', 'time_agg', 'source'])
    ds_values = ds_values.transpose('time','id', 'spatial_agg', 'timestep', 'time_agg', 'source')
    return xr.merge([ds_values, ds_time])


def postprocess(ds, data_type):
    ds = ds.chunk({'id': 1, 'time_agg': 1, 'timestep': 1, 'time': -1, 'spatial_agg': 1})

    for var in ds.variables:
        ds[var].encoding.clear()

    ds['start_date'] = ds[data_type].dropna('time').isel(time=[0]).idxmax('time').expand_dims({'variable': [data_type]})
    ds['end_date'] = ds[data_type].dropna('time').isel(time=[-1]).idxmax('time').expand_dims({'variable': [data_type]})
    ds = ds.set_coords(['start_date','end_date'])

    ds['name'] = ds.name.astype(object)
    ds['id'] = ds.id.astype(object)
    ds['latitude'] = ds.latitude.astype('float32')
    ds['longitude'] = ds.longitude.astype('float32')
    ds['province'] = ds.province.astype(object)
    ds['drainage_area'] = ds.drainage_area.astype('float32')
    ds['regulated'] = ds.regulated.astype(object)
    ds['source'] = ds.source.astype(object)
    ds['timestep'] = ds.timestep.astype(object)
    ds['spatial_agg'] = ds.spatial_agg.astype(object)
    ds['time_agg'] = ds.time_agg.astype(object)
    ds['variable'] = ds.variable.astype(object)
    ds[data_type] = ds[data_type].astype('float32')

    ds[data_type] = ds[data_type].transpose('id', 'time', 'spatial_agg', 'timestep', 'time_agg', 'source')
    
    return ds

@task
def get_available_stations():
    # fetch reference data
    stations_list = get_available_stations_from_cehq()
    return stations_list[0:2]



@task
def store_station(input_path):
    """

    :param station:
    :return:
    """
    CEHQ_URL = "https://www.cehq.gouv.qc.ca/depot/historique_donnees/fichier/"
    output_path = '/tmp/deh/raw'

    try:
        station_id = input_path.strip()
        station = StationParserCEHQ(url=CEHQ_URL + os.path.basename(station_id) + '_Q.txt')
        ds_meta = prepare_id_metadata(station)
        ds_values = prepare_values(station)
        ds = xr.merge([ds_values, ds_meta])
        ds = postprocess(ds, station.data_type)
        ds.to_zarr(os.path.join(output_path, station_id + "_Q"), consolidated=True)
    except Exception:
        pass
    try:
        station_id = input_path.strip()
        station = StationParserCEHQ(CEHQ_URL + os.path.basename(station_id) + '_N.txt')
        ds_meta = prepare_id_metadata(station)
        ds_values = prepare_values(station)
        ds = xr.merge([ds_values, ds_meta])
        ds = postprocess(ds, station.data_type)
        ds.to_zarr(os.path.join(output_path, station_id +  "_N"), consolidated=True)
    except Exception:
        pass

@task
def merge_stations():
    ds = xr.open_mfdataset('/tmp/deh/raw/*', 
                    engine='zarr',
                    consolidated=True, 
                    parallel=True)
    ds = ds.chunk({'id': 1, 'time_agg': 1, 'timestep': 1, 'time': -1, 'spatial_agg': 1})
    for var in ds.variables:
        ds[var].encoding.clear()

    ds['name'] = ds.name.astype(object)
    ds['id'] = ds.id.astype(object)
    ds['latitude'] = ds.latitude.astype('float32')
    ds['longitude'] = ds.longitude.astype('float32')
    ds['province'] = ds.province.astype(object)
    ds['drainage_area'] = ds.drainage_area.astype('float32')
    ds['regulated'] = ds.regulated.astype(object)
    ds['source'] = ds.source.astype(object)
    ds['timestep'] = ds.timestep.astype(object)
    ds['spatial_agg'] = ds.spatial_agg.astype(object)
    ds['time_agg'] = ds.time_agg.astype(object)
    ds['variable'] = ds.time_agg.astype(object)
    
        
    ds.to_zarr('/tmp/deh/timeseries', consolidated=True)

@ task()
def push_data_to_bucket():
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path='/tmp/deh/timeseries')
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    fs.put(target.root_path, os.path.dirname(Config.DEH_ZARR_BUCKET), recursive=True)
    shutil.rmtree(target.root_path)

if __name__ == '__main__':
    with Flow("DEH-ETL", executor=DaskExecutor()) as flow:

        stations_list = get_available_stations()
        store = store_station.map(stations_list)
        merged = merge_stations(upstream_tasks=[store])
        push_data_to_bucket(upstream_tasks=[merged])

    flow.run()