from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import os
import zipfile
from config import Config
# from hydrodatahub.database.elements.bassin import Bassin
import geopandas as gpd
from pipeline.utils import get_url_paths, SQLAlchemyDBConnection, verify_hydat_available
from pipeline.models.basin import Basin
import multiprocessing
import dask
import xarray as xr

@dataclass
class StationParserHYDAT:
    """
    """

    station_number: str
    data_type: str = "Flow"

    @property
    def _file_content(self) -> tuple:
        """
        :param station_number:
        :param filename:
        :return:
        """
        verify_hydat_available()

        with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
            meta = MetaData(bind=db.session.bind)
            meta.reflect(bind=db.session.bind)

            return db.session.query(meta.tables['STATIONS'], meta.tables['STN_REGULATION']) \
                .filter('STATION_NUMBER' == 'STATION_NUMBER') \
                .filter_by(STATION_NUMBER=self.station_number) \
                .first()

    @property
    def area(self) -> float:
        return self._file_content.DRAINAGE_AREA_GROSS

    @property
    def station_name(self) -> float:
        return self._file_content.STATION_NAME

    @property
    def regulation(self)-> str:
        """

        :return:
        """
        regulation_type = 'Undefined'
        regulation_options: dict = {True: "Regulated",
                                    False: "Natural"}

        with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
            meta = MetaData(bind=db.session.bind)
            meta.reflect(bind=db.session.bind)

            regulation_key = db.session.query(meta.tables['STN_REGULATION']) \
                .filter_by(STATION_NUMBER=self.station_number) \
                .first()

            if regulation_key:
                if not regulation_key.REGULATED is None:
                    regulation_type = regulation_options[regulation_key.REGULATED]

        return regulation_type

    @property
    def latitude(self)-> float:
        return self._file_content.LATITUDE

    @property
    def longitude(self)-> float:
        return self._file_content.LONGITUDE

    @property
    def values(self) -> pd.DataFrame:
        """
        :param type_serie:
        :return:
        """

        data_type: dict = {"Flow": {"table": "DLY_FLOWS",
                                    "get_flow": True},
                           "Level": {"table": "DLY_LEVELS",
                                    "get_flow": False},}

        with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
            meta = MetaData(bind=db.session.bind)
            meta.reflect(bind=db.session.bind)

            df = hydat_sqlite_convert(
                pd.DataFrame(
                    db.session.query(meta.tables[data_type[self.data_type]['table']]) \
                        .filter_by(STATION_NUMBER=self.station_number) \
                        .all()), data_type[self.data_type]['get_flow'], False).drop(columns=['STATION_NUMBER'])

        df.columns = ['date', 'value']
        df['value'] = df['value'].apply(pd.to_numeric, errors='coerce')
        df = df.set_index('date')
        df.index = pd.to_datetime(df.index)
        # df.index = df.index.tz_localize("America/Montreal", ambiguous='infer', nonexistent='shift_forward') \
        #     .tz_convert("UTC")
        df.sort_index(inplace=True)
        df = df.dropna()
        df = df.loc[~df.index.duplicated(keep='first')]
        all_days = pd.date_range('1800-01-01', '2050-01-01', freq='D')
        df = df.reindex(all_days, fill_value=np.nan)
        return df

    @property
    def start_date(self) -> str:
        """

        :return:
        """
        return self.values['value'].dropna().index[0]

    @property
    def end_date(self) -> str:
        """

        :return:
        """
        return self.values['value'].dropna().index[-1]

    @property
    def units(self) -> str:
        return "cms" if self.data_type == "Flow" else "m"

    @property
    def time_step(self) -> str:
        return "1D"

    @property
    def aggregation(self) -> str:
         return "mean"

    @property
    def operator(self) -> str:
        """

        :return:
        """
        AGENCY_EN = 'Undefined'
        with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
            meta = MetaData(bind=db.session.bind)
            meta.reflect(bind=db.session.bind)

            OPERATOR_ID = db.session.query(meta.tables['STATIONS']) \
                .filter_by(STATION_NUMBER=self.station_number) \
                .first().OPERATOR_ID
            if OPERATOR_ID:
                AGENCY_EN = db.session.query(meta.tables['AGENCY_LIST']) \
                    .filter_by(AGENCY_ID=OPERATOR_ID) \
                    .first().AGENCY_EN
        return AGENCY_EN

    @property
    def province(self) -> str:
        return self._file_content.PROV_TERR_STATE_LOC


def hydat_sqlite_convert(df, get_flow, to_plot):
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
    header = "^FLOW\\d+" if get_flow == True else "^LEVEL\\d+"

    dly = df[["STATION_NUMBER", "YEAR", "MONTH"]]
    dly.columns = ["STATION_NUMBER", "YEAR", "MONTH"]

    # value.cols = df.columns[df.filter(regex="^FLOW\\d+")]
    # filter  sur les FLOW
    value = df.filter(regex=header)
    valuecols = value.columns
    # print(dlydata.shape)
    # now melt the offline_data frame for offline_data and flags
    dlydata = pd.melt(df, id_vars=["STATION_NUMBER", "YEAR", "MONTH"], value_vars=valuecols)

    if get_flow is True:
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
    header_sym = "^FLOW_SYMBOL\\d+" if get_flow == True else "^LEVEL_SYMBOL\\d+"
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

    if to_plot == 1:
        dlytoplot.plot()
        return dlytoplot
    else:
        return dlydata


def get_available_stations_from_hydat(regions_list=['QC', 'ON', 'NB', 'NL']):
    """
    Get list of all available stations from cehq with station's number and name as value
    """
    verify_hydat_available()

    with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
        df = pd.read_sql_query("SELECT * FROM STATIONS", db.session.bind)
        df = df[df['PROV_TERR_STATE_LOC'].isin(regions_list)]['STATION_NUMBER']

    return df.drop_duplicates().reset_index().drop(columns=['index'])['STATION_NUMBER'].tolist()


def import_hydat_to_parquet(station_number):
    """
    :param station:
    :return:
    """

    # if not is_id_bassin_in_db(station):

    try:
        project_root = '/tmp'
        data_dir = os.path.join(project_root, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        zarr_dir = os.path.join(data_dir, 'zarr')
        if not os.path.exists(zarr_dir):
            os.makedirs(zarr_dir)
        # TODO : Verify if Flow exists
        sta = StationParserHYDAT(station_number, 'Flow')
        b = Basin(sta)

        fname_basin = os.path.join(data_dir, 'basin.parquet')
        # Basin
        if os.path.isfile(fname_basin):

            # Verify if index exists
            if not (station_number in pd.read_parquet(fname_basin, engine='pyarrow').index):
                b.basin_table.to_parquet(fname_basin,
                                         engine='fastparquet',
                                         compression='gzip',
                                         append=True)
        else:
            b.basin_table.to_parquet(fname_basin,
                                     engine='fastparquet',
                                     compression='gzip')

        fname_context = os.path.join(data_dir, 'context.parquet')
        # Context
        if os.path.isfile(fname_context):
            b.context_table.to_parquet(fname_context,
                                       engine='fastparquet',
                                       compression='gzip',
                                       append=True)
        else:
            b.context_table.to_parquet(fname_context,
                                       engine='fastparquet',
                                       compression='gzip')

        # fname_values = '/home/slanglois/Documents/HQ/data/values/{}.parquet'.format(station)
        # if os.path.isfile(fname_values):
        #     b.values_table.to_parquet(fname_values,
        #                              engine='fastparquet',
        #                              compression='gzip',
        #                              append=True)
        # else:
        #     b.values_table.to_parquet(fname_values,
        #                                engine='fastparquet',
        #                                compression='gzip')

        if os.path.exists(zarr_dir):
            b.values_table.to_xarray().to_zarr(zarr_dir,
                                               mode='a')
        else:
            b.values_table.to_xarray().to_zarr(zarr_dir)

        print(station_number)
    except Exception as e:
        print(e)


def verify_data_type_exists(station_number, data_type):

    data_type_options = {'Flow': 'Q',
                         'Level' : 'H'}

    hydat_data_type = data_type_options[data_type]

    # import_hydat_to_parquet(station)
    with SQLAlchemyDBConnection(Config.SQLITE_LOCAL_FILE) as db:
        meta = MetaData(bind=db.session.bind)
        meta.reflect(bind=db.session.bind)

        data_type = db.session.query(meta.tables['STN_DATA_RANGE']) \
            .filter_by(STATION_NUMBER=station_number) \
            .all()
    data_type_list = [value.DATA_TYPE for value in data_type]
    return hydat_data_type in data_type_list


def create_basin_table(station_number):
    print(station_number)
    sta = StationParserHYDAT(station_number, 'Flow')
    return Basin(sta).basin_table


if __name__ == "__main__":

    storage_options = {"client_kwargs": {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                                         'region_name': 'us-east-2'}}

    project_root = '/tmp'
    data_dir = os.path.join(project_root, 'data')
    stations_list = get_available_stations_from_hydat()
    #
    # results = []
    for station_number in stations_list[0:10]:
        if verify_data_type_exists(station_number, 'Flow'):
            import_hydat_to_parquet(station_number)

    df = pd.read_parquet(os.path.join(data_dir, 'basin.parquet'), engine='pyarrow')
    df.to_parquet('s3://hydrology/timeseries/sources/hydat/basin.parquet',
                  engine='fastparquet',
                  compression='gzip',
                  storage_options=storage_options)
    df = pd.read_parquet(os.path.join(data_dir, 'context.parquet'), engine='pyarrow')
    df.to_parquet('s3://hydrology/timeseries/sources/hydat/context.parquet',
                  engine='fastparquet',
                  compression='gzip',
                  storage_options=storage_options)

    import subprocess

    bucket_source = os.path.join(data_dir, 'zarr')
    bucket_sink = "s3://hydrology/timeseries/sources/hydat/values.zarr "
    endpoint_url = 'https://s3.us-east-2.wasabisys.com'
    region='us-east-2'
    aws_command = "aws s3 sync {} {} --endpoint-url={} --region={}".format(bucket_source,
                                                                           bucket_sink,
                                                                           endpoint_url,
                                                                           region)
    print(aws_command)
    subprocess.call(aws_command, shell=True)
    # import zarr
    # zarr.consolidate_metadata(os.path.join(data_dir, 'zarr'))
    # ds = xr.open_zarr(os.path.join(data_dir, 'zarr'), consolidated=True)
    # ds['data_type'] = ds.data_type.astype('str')
    #
    # # Wasabi cloud storage configurations
    # client_kwargs = {'endpoint_url': 'https://s3.us-east-1.wasabisys.com',
    #                  'region_name': 'us-east-1'}
    # config_kwargs = {'max_pool_connections': 30}
    # import s3fs
    # s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs,
    #                        config_kwargs=config_kwargs,
    #                        profile="default")  # public read
    # store = s3fs.S3Map(root='s3://hydrology/timeseries/sources/hydat/values.zarr',
    #                    s3=s3)
    # ds.to_zarr(store, mode='w')






    #     if verify_data_type_exists(station_number, 'Flow'):
    #         print(station_number)
    #         b = create_basin_table(station_number)
    #         results.append(b)
    # # results = list(dask.compute(*results))
    # print(type(results[0]))
    # dfs = pd.concat(results, axis=0)
    # print(dfs.head)
    #
    # fname_basin = '/home/slanglois/Documents/HQ/data/basin/basin.parquet'
    # # dfs = dfs.repartition(npartitions=1)
    # dfs.to_parquet(fname_basin,
    #                engine='fastparquet',
    #                compression='gzip')





    #
    #
    # # pool.map(import_hydat_to_parquet, stations_list)
    #




    # import_hydat_to_zarr(stations_list.loc[2].values[0])

    # sta = StationParserHYDAT('01AK002', 'Flow')
    # b = Basin(sta)
    # b.basin_table
    # b.context_table
    # b.values_table.to_xarray()


    # # import_hydat_to_sql(stations_list[1])
    #
    #
    # station = StationParserHYDAT('02OD018', 'Flow')
    # b = Basin(sta)

#     a = b.values_table.to_xarray()
#     c = b.basin_table.to_xarray()
#     a.to_zarr('/home/slanglois/Documents/HQ/data/zarr')
#     import xarray as xr
#     import glob
#
#
#     ds1 = xr.open_zarr('/home/slanglois/Documents/HQ/data/zarr')
#     ds2 = xr.merge([ds1, a.rename({'02OD018':'value2'})])
#     print(ds2)
#     mtimes = {}
#     contents = list(glob.glob("/home/slanglois/Documents/HQ/data/zarr/*"))
#     for c in contents:
#         mtimes.update({c: os.path.getmtime(c)})
#     ds2_dropped = ds2.drop(["data_type","date"])
#     print(ds2_dropped)
#     # # d.to_zarr('/home/slanglois/Documents/HQ/data/zarr', append_dim='')
#     a.rename({'02OD018':'value2'}).to_zarr('/home/slanglois/Documents/HQ/data/zarr', mode='a',
# )

    # a.to_zarr('/home/slanglois/Documents/HQ/data/zarr', mode='a', append_dim='station_number')

    # for c in contents:
    #     assert os.path.getmtime(c) == mtimes[c]
    # print(b.context_table)
    # b.basin_table.set_index('station_number').to_parquet('/home/slanglois/Documents/HQ/data/basin/basins.parquet', engine='fastparquet',
    #                          compression='gzip', append=True)

# from hydrodatahub.models import bassin
#
#
# def is_id_bassin_in_db(station):
#     with SQLAlchemyDBConnection(Config.SQLALCHEMY_DATABASE_URI) as db:
#         if db.session.query(bassin).filter_by(nom_equiv=station).first() is not None:
#             return True
#         else:
#             return False
#
#
# def import_hydat_to_sql(station):
#     """
#     :param station:
#     :return:
#     """
#     hydat_sqlite = 'sqlite:///' + \
#                    os.path.join(Config.SQLITE_HYDAT_FOLDER,
#                                 "Hydat.sqlite3")
#     print(station.strip())
#
#     if not is_id_bassin_in_db(station):
#
#         try:
#             sta = StationParserHYDAT(filename=hydat_sqlite,
#                                      station_number=station.strip(),
#                                      type_serie='Debit')
#             b = Bassin(sta)
#             b.to_sql()
#         except Exception:
#             pass