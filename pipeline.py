import sqlalchemy
import pandas as pd
from prefect import task, Flow, case
import requests
from bs4 import BeautifulSoup
import os
import urllib
import zipfile
import numpy as np
import xarray as xr
import glob
import pandas as pd
import numpy as np
import time
import fsspec
import rioxarray as rio
import geopandas as gpd
from dask.distributed import Client
from pangeo_forge_recipes.storage import FSSpecTarget
from fsspec.implementations.local import LocalFileSystem
import zarr
import shutil

from config import Config

HYDAT_WWW = 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/'
HYDAT_LOCAL_FILE = "/tmp/hydat/Hydat.sqlite3"
HYDAT_BASE_URL = 'https://s3.us-east-1.wasabisys.com/hydrometric/canada/hydat/sqlite/Hydat_sqlite3_20221024.zip'

columns = \
{'STATION_NUMBER': 'id',
 'STATION_NAME': 'name',
 'PROV_TERR_STATE_LOC': 'province',
 'LONGITUDE': 'longitude',
 'LATITUDE': 'latitude',
 'DRAINAGE_AREA_GROSS':'drainage_area',
 'REGULATED': 'regulated',
 'OPERATOR_ID': 'source'
 }

data_type = {'Q': 'flow',
             'H': 'level'}

regulated = {0: 'Natural',
             1: 'Influenced'}

# client = Client()

@task
def get_url_paths(HYDAT_WWW):
    dates = pd.date_range(pd.Timestamp.now() - pd.Timedelta(days=365), pd.Timestamp.now())
    dates_r200 = []
    for date in dates:
        template = f"Hydat_sqlite3_{date.strftime('%Y%m%d')}.zip"
        url = os.path.join(HYDAT_WWW, template)
        response = requests.request('HEAD', url)
        if response.status_code == 200:
            dates_r200.append(date)
    current_date = max(dates_r200)

    template = f"Hydat_sqlite3_{current_date.strftime('%Y%m%d')}.zip"
    url = os.path.join(HYDAT_WWW, template)
    return url

@task
def download_hydat(url):
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall('/tmp/hydat')


def read_stations_table(path, columns):
    db_engine = sqlalchemy.create_engine(f'sqlite:///{path}')
    df_stations = pd.read_sql(f"select * from stations",
                                  con=db_engine)
    df_regulation = pd.read_sql(f"select station_number, regulated from stn_regulation",
                                    con=db_engine)

    return df_stations.merge(df_regulation, how='left')


def postprocess_stations_table(df, columns):
    """

    """
    db_engine = sqlalchemy.create_engine(f'sqlite:///{HYDAT_LOCAL_FILE}') 

    source = pd.read_sql(f"select * from AGENCY_LIST",
                            con=db_engine)[['AGENCY_ID', 'AGENCY_EN']] \
        .set_index('AGENCY_ID').to_dict()['AGENCY_EN']

    # Replace by effective drainage area when gross is empty and remove effective column
    df.DRAINAGE_AREA_GROSS.fillna(df.DRAINAGE_AREA_EFFECT, inplace=True)
    df.drop(columns='DRAINAGE_AREA_EFFECT', inplace=True)

    # keep only relevant columns and correct names
    df = df.loc[:, columns.keys()]
    df = df.rename(columns=columns)

    df['regulated'] = df['regulated'].apply(lambda x: regulated[x] if x in regulated.keys() else 'Undefined')
    df['source'] = df['source'].apply(
        lambda x: source[x] if x in source.keys() else 'WATER SURVEY OF CANADA (DOE) (CANADA)')

    return df


def hydat_sqlite_convert(df, data_type, to_plot):
    # Deux ou plus stations
    # station_number = "'02TC001' OR STATION_NUMBER='02TC002'"
    # station1 = "'02TC001'"
    # station2 = "'02TC002'"
    # qstr = "SELECT * FROM DLY_FLOWS WHERE STATION_NUMBER=%s OR STATION_NUMBER=%s\n" %(station1, station2)
    # qstr = SELECT * FROM DLY_FLOWS WHERE STATION_NUMBER='02TC001' OR STATION_NUMBER='02TC002'
    # ATTENTION PEUT ETRE TRES LONG

    # print(df)
    # if get_flow == True:
    #     header = "^FLOW\\d+"
    # else:
    #     header = "^LEVEL\\d+"
    header = "^FLOW\\d+" if data_type == 'DLY_FLOWS' else "^LEVEL\\d+"

    dly = df[["STATION_NUMBER", "YEAR", "MONTH"]]
    dly.columns = ["STATION_NUMBER", "YEAR", "MONTH"]

    # value.cols = df.columns[df.filter(regex="^FLOW\\d+")]
    # filter  sur les FLOW
    value = df.filter(regex=header)
    valuecols = value.columns
    # print(dlydata.shape)
    # now melt the offline_data frame for offline_data and flags
    dlydata = pd.melt(df, id_vars=["STATION_NUMBER", "YEAR", "MONTH"], value_vars=valuecols)

    if data_type == 'DLY_FLOWS':
        dlydata["DAY"] = dlydata['variable'].apply(lambda x: np.int8(x[4:]))
    else:
        dlydata["DAY"] = dlydata['variable'].apply(lambda x: np.int8(x[5:]))
    # flowvariable = dlydata["variable"]
    # days = [x[4:6] for x in flowvariable]
    # dlydata["DAY"] = list(map(int, days))
    # censor ambiguous dates (e.g., 31st day for Apr, Jun, Sept, Nov)
    d = dlydata.loc[dlydata["MONTH"].isin([4, 6, 9, 11]) & (dlydata["DAY"] > 30)]
    d30 = d
    # print(d.index[:])
    # print(len(d))#

    if len(d) > 0:
        dlydata = dlydata.drop(d.index).reset_index(drop=True)
    # print(dlydata.shape)

    d = dlydata.loc[(dlydata["MONTH"].isin([2]) &
                     pd.to_datetime(dlydata["YEAR"], format='%Y').dt.is_leap_year &
                     (dlydata["DAY"] > 29))]
    if len(d) > 0:
        dlydata = dlydata.drop(d.index).reset_index(drop=True)
    d29 = d
    # print(dlydata.shape)

    d = dlydata.loc[(dlydata["MONTH"].isin([2]) &
                     ~pd.to_datetime(dlydata["YEAR"], format='%Y').dt.is_leap_year.values &
                     (dlydata["DAY"] > 28))]
    # print(d)
    if len(d) > 0:
        dlydata = dlydata.drop(d.index).reset_index(drop=True)
    d28 = d
    # print(dlydata.shape)
    # print(valuecols)

    # ----------------------------------SYMBOL--------------------------------------------------
    header_sym = "^FLOW_SYMBOL\\d+" if data_type == 'DLY_FLOWS' else "^LEVEL_SYMBOL\\d+"
    flag = df.filter(regex=header_sym)
    flagcols = flag.columns
    # print(flagcols)
    # ordonner les flag dans un dataframe
    dlyflags = pd.melt(df, id_vars=["STATION_NUMBER", "YEAR", "MONTH"], value_vars=flagcols)

    if len(d30) > 0:
        dlyflags = dlyflags.drop(d30.index).reset_index(drop=True)
    # print(dlyflags.shape)

    if len(d29) > 0:
        dlyflags = dlyflags.drop(d29.index).reset_index(drop=True)
    # print(dlyflags.shape)

    if len(d28) > 0:
        dlyflags = dlyflags.drop(d28.index).reset_index(drop=True)
    # print(dlyflags.shape)
    # -----------------------------------END SYMBOL---------------------------------------------

    # transform date
    dlydata.insert(loc=1, column='DATE', value=pd.to_datetime(dlydata[['YEAR', 'MONTH', 'DAY']]))
    # ---------------------------------plot the dataframe--------------------------------------
    dlytoplot = dlydata[['DATE', 'value']].set_index('DATE')
    dlydata = dlydata.drop(['YEAR', 'MONTH', 'DAY', 'variable'], axis=1)

    dlydata.columns = ['id', 'time', 'value']

    if to_plot == 1:
        dlytoplot.plot()
        return dlytoplot
    else:
        return dlydata


def prepare_id_metadata():
    """
    Prepare id metadata

    """
    db_engine = sqlalchemy.create_engine(f'sqlite:///{HYDAT_LOCAL_FILE}')

    query = f"""
    select STATION_NUMBER as id,
    DATA_TYPE as variable 
    from STN_DATA_RANGE"""

    df_data_type = pd.read_sql(query,
                                con=db_engine)

    station_numbers = df_data_type \
        .loc[df_data_type['variable'] \
        .isin(['Q', 'H'])] \
        .id \
        .unique()

    df = read_stations_table(HYDAT_LOCAL_FILE, columns)
    df = postprocess_stations_table(df, columns)
    df = df.loc[df.id.isin(station_numbers)]

    ds = df.set_index(['id']).to_xarray()

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
    


    return ds.set_coords(("name", "province", "longitude",
                          "latitude", "drainage_area", "regulated",
                          "source"))


def store_values_by_year(year, data_type):
    db_engine = sqlalchemy.create_engine(f'sqlite:///{HYDAT_LOCAL_FILE}')
    time.sleep(0.1)
    print(year)
    raw_data = pd.read_sql(f"select * from {data_type} where year = {year}",
                               con=db_engine)
    time.sleep(0.5)
    #     filtered_data = raw_data[raw_data['STATION_NUMBER'].isin(stations_filter)]
    transformed_data = hydat_sqlite_convert(raw_data, data_type, False)
    transformed_data['time_agg'] = 'mean'
    transformed_data['variable'] = 'streamflow' if data_type == 'DLY_FLOWS' else 'level'
    transformed_data['timestep'] = 'D'
    transformed_data['spatial_agg'] = 'watershed' if data_type == 'DLY_FLOWS' else 'point'
    transformed_data['source'] = "Water Survey of Canada"

    ds_batch = transformed_data.drop_duplicates() \
        .set_index(['id', 'time', 'variable', 'spatial_agg', 'timestep', 'time_agg','source']).to_xarray()
    
    rename_key = 'streamflow' if data_type == 'DLY_FLOWS' else 'level'
    
    ds_batch = ds_batch.rename({'value': rename_key})
    
    ds_batch.to_zarr(f'/tmp/hydat/values/{data_type}_{year}', mode='w', consolidated=True)
    time.sleep(0.1)
    return ds_batch

@task
def process_data():

    for year in range(1860, 2023):
        store_values_by_year(year, 'DLY_FLOWS')

    for year in range(1860, 2023):
        store_values_by_year(year, 'DLY_LEVELS')



@task
def create_hydat_dataset():
    zarr_paths = glob.glob('/tmp/hydat/values/*')
    ds = xr.open_mfdataset(zarr_paths,
                           engine='zarr',
                           consolidated=True)
    ds = xr.merge([prepare_id_metadata(), ds])

    ds = ds.chunk({'id': 1, 'variable': 1, 'time_agg': 1, 'timestep': 1, 'time': -1, 'spatial_agg': 1})

    for var in ds.variables:
        ds[var].encoding.clear()

    times = ds.time.values
    ts = pd.to_datetime(times)
    d = ts.strftime('%Y-%m-%d')
    e = d.values
    e[0] = np.nan

    def get_first_date(x, axis):
        return e[(~np.isnan(x)).argmax(1)]

    def get_last_date(x, axis):
        return e[(~np.isnan(x)).cumsum(1).argmax(1)]

    #     da = x.dropna('time', how='all')
    #     if len(da.time)>0:
    #         return da.isel(time=0)
    #     else:
    #         np.nan

    #start_date = ds.reduce(get_first_date, dim='time').value
    #end_date = ds.reduce(get_last_date, dim='time').value

    start_date_list = []
    end_date_list = []
    for var in ds.keys():
        start_date_list.append(ds[var].reduce(get_first_date, dim='time').rename('start_date'))
        end_date_list.append(ds[var].reduce(get_last_date, dim='time').rename('end_date'))
    
    start_date = xr.merge(start_date_list).start_date
    end_date = xr.merge(end_date_list).end_date

    ds['start_date'] = start_date
    ds['end_date'] = end_date

    ds = ds.set_coords(('start_date', 'end_date'))

    ds['variable'] = ds.variable.astype('str')
    ds['name'] = ds.name.astype('str')
    ds['id'] = ds.id.astype('str')
    ds['latitude'] = ds.latitude.astype('float32')
    ds['longitude'] = ds.longitude.astype('float32')
    ds['province'] = ds.province.astype('str')
    ds['drainage_area'] = ds.drainage_area.astype('float32')
    ds['regulated'] = ds.regulated.astype('str')
    ds['source'] = ds.source.astype('str')
    ds['timestep'] = ds.timestep.astype('str')
    ds['spatial_agg'] = ds.spatial_agg.astype('str')
    ds['time_agg'] = ds.time_agg.astype('str')
    ds['source'] = ds.source.astype('str')
    ds['streamflow'] = ds.streamflow.astype('float32')
    ds['level'] = ds.level.astype('float32')

    ds.attrs[
        "institution"
    ] = "Water Survey of Canada"
    ds.attrs[
        "source"
    ] = "Hydrometric data <https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/>"
    ds.attrs["redistribution"] = "Redistribution policy unknown."

    ds.to_zarr('/tmp/hydat/zarr', consolidated=True, mode='w')
    return [ds.time[0].values, ds.time[-1].values]


@task
def consolidate_coords():

    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path='/tmp/hydat/zarr')
    ds = xr.open_zarr(target.get_mapper(), consolidated=True)

    group = zarr.open(target.get_mapper(), mode="a")
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/214
    # filter out the dims from the array metadata not in the Zarr group
    # to handle coordinateless dimensions.
    coords = list(set(group.keys()) - set(ds.keys()) - set(['time']))

    dims = (dim for dim in coords)
    for dim in dims:
        arr = group[dim]
        attrs = dict(arr.attrs)
        data = arr[:]

        # This will generally use bulk-delete API calls
        target.rm(dim, recursive=True)

        new = group.array(
            dim,
            data,
            chunks=arr.shape,
            dtype=arr.dtype,
            compressor=arr.compressor,
            fill_value=arr.fill_value,
            order=arr.order,
            filters=arr.filters,
            overwrite=True,
        )
        new.attrs.update(attrs) 

@task()
def post_process_dims(dates):
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path='/tmp/hydat/zarr')

    store = target.get_mapper()
    ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    ds['time'] = pd.date_range(dates[0], dates[1])
    ds.to_zarr(target.get_mapper(), compute=False, mode='a')
    zarr.consolidate_metadata(store)

@ task()
def push_data_to_bucket():
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path='/tmp/hydat/zarr')
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    fs.put(target.root_path, os.path.dirname(Config.HYDAT_ZARR_BUCKET), recursive=True)
    shutil.rmtree(target.root_path)

with Flow("Hydat-ETL") as flow:
    url = get_url_paths(HYDAT_WWW)
    print(url)
    path = download_hydat(url)
    processed = process_data(upstream_tasks=[path])
    dates = create_hydat_dataset(upstream_tasks=[processed])
    consolidated = consolidate_coords(upstream_tasks=[dates])
    processed = post_process_dims(dates, upstream_tasks=[consolidated])
    push_data_to_bucket(upstream_tasks=[processed])

    

flow.run()