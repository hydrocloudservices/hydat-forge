from urllib.request import urlopen
import pandas as pd
import re
import numpy as np
import os
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import datetime


class StationParserCEHQ:
    """

    """

    def __init__(self,
                 url: str,
                 encoding: str = "ISO-8859-1",

                 ):

        self.filename: str = url
        self.encoding: str = encoding
        self._content: list = self.open_file(url, encoding)
        self.drainage_area: float = float(self._content[3][15:].split()[0])
        self.id: str = self._content[2].split()[1]
        self.latitude: float = self.get_lat_long()[0]
        self.longitude: float = self.get_lat_long()[1]
        self.start_date: str = self.get_start_end_date()[0]
        self.end_date: str = self.get_start_end_date()[1]
        self.data_type: str = self.get_type_serie()[0]
        self.timestep: str = 'D'
        self.time_agg: str = "mean"
        self.spatial_agg: str = 'watershed' if self.data_type == 'streamflow' else 'point'
        self.province: str = "QC"

    @staticmethod
    def open_file(filename, encoding) -> list:
        """

        :param filename:
        :param encoding:
        :return:
        """
        with urlopen(filename) as f:
            return f.read().decode(encoding).splitlines()

    @property
    def name(self) -> str:
        """

        :param filename:
        :param encoding:
        :return:
        """

        full_description_line = self._content[2].split('-')[0]
        station_car_index = full_description_line.find(self.id)
        nom_station = full_description_line[station_car_index+6:].strip()
        return nom_station
    
    @property
    def source(self) -> str:
        """

        :param filename:
        :param encoding:
        :return:
        """
        return "Ministère de l’Environnement, de la Lutte " + \
        "contre les changements climatiques, de la Faune et des Parcs"
    
    @property
    def regulated(self) -> str:
        """

        :param filename:
        :param encoding:
        :return:
        """
        regulated: str = self._content[3][15:].split()[-1]
        
        if regulated in ['Naturel']:
            regulated = 'Natural'
        
        elif regulated in ['Influence journellement', 'journellement', 'Influencé journellement']:
            regulated = 'Influenced (daily)'
            
        elif regulated in ['Influence mensuellement', 'mensuellement', 'Influencé mensuellement']:
            regulated = 'Influenced (monthly)'
        else:
            regulated = 'Influenced'
            
        return regulated
        
    def get_lat_long(self) -> list:
        """

        :param filename:
        :param encoding:
        :return:
        """
        lat_long = []
        latlng_coordinates = self._content[4][23:-1].split()
        if len(latlng_coordinates) < 5:
            latlng_coordinates = [x for x in latlng_coordinates if x]
            lat_long =  [float(latlng_coordinates[0]), float(latlng_coordinates[2])]
        elif len(latlng_coordinates) >= 5:
            latlng_coordinates = [re.sub(r'[^-\d]', '', coord) for coord in latlng_coordinates]
            latlng_coordinates = [x for x in latlng_coordinates if x]
            lat_long = [float(latlng_coordinates[0]) +
                        float(latlng_coordinates[1]) / 60 +
                        float(latlng_coordinates[2]) / 3600,
                        float(latlng_coordinates[3]) -
                        float(latlng_coordinates[4]) / 60 -
                        float(latlng_coordinates[5]) / 3600]
        return lat_long

    @property
    def values(self) -> pd.DataFrame:
        """

        :param filename:
        :param encoding:
        :return:
        """
        df = pd.DataFrame(self._content[22:]).fillna(np.nan)[0].str.split(" +", expand=True).iloc[:, 1:3]
        df.columns = ['time', 'value']
        df['value'] = df['value'].apply(pd.to_numeric, errors='coerce')
        df = df.set_index('time')
        df.index = pd.to_datetime(df.index)
#         df.index = df.index.tz_localize("Etc/GMT+5", ambiguous='infer', nonexistent='shift_forward')
        df.sort_index(inplace=True)
        return df

    def get_start_end_date(self) -> list:
        """

        :param filename:
        :param encoding:
        :return:
        """
        debut = self.values['value'].dropna().index[0]
        fin = self.values['value'].dropna().index[-1]
        return [debut, fin]

    def get_type_serie(self) -> list:
        """

        :param filename:
        :param encoding:
        :return:
        """
        type_serie_id = self.filename.split('/')[-1].split('.')[0][-1]
        return ["streamflow", "m3/s"] if type_serie_id == "Q" else ['level', "m"]

def simple_get(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    """
    Prints log errors
    """
    print(e)


def get_available_stations_from_cehq(url='https://www.cehq.gouv.qc.ca/hydrometrie/historique_donnees/ListeStation.asp?regionhydro=$&Tri=Non',
                                     regions_list=["%02d" % n for n in range(0, 13)]):
    """
    Get list of all available stations from cehq with station's number and name as value
    """
    print('[' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '] Getting all available stations...')

    stations = []
    for region in regions_list:
        file_url = url.replace('$', region)
        raw_html = simple_get(file_url)
        html = BeautifulSoup(raw_html, 'html.parser')

        for list_element in (html.select('area')):
            if list_element['href'].find('NoStation') > 0:
                try:
                    station_infos = list_element['title'].split('-', 1)[0]
                    stations.append(station_infos)

                except RequestException as e:
                    log_error('Error during requests to {0} : {1}'.format(url, str(e)))

    print('[' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '] ' + str(len(stations)) + ' available stations...')
    print('[' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '] Getting all available stations...done')
    return stations
