import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import urllib.request
from pipeline.config import Config
import zipfile


class SQLAlchemyDBConnection(object):
    """SQLAlchemy database connection"""

    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.session = None

    def __enter__(self):
        engine = create_engine(self.connection_string)
        Session = sessionmaker()
        self.session = Session(bind=engine)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()


def get_url_paths(url,
                  ext='',
                  params={}):
    response = requests.get(url, params=params)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    parent = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    if len(parent)==2:
        parent = parent[1]
    return parent


def verify_hydat_available():
    url_path = get_url_paths(Config.HYDAT_URL, Config.HYDAT_FORMAT)
    basename = os.path.basename(url_path)
    local_zip_filename = os.path.join('/tmp', basename)

    if not os.path.isfile(Config.HYDAT_LOCAL_FILE) or os.path.getsize(Config.HYDAT_LOCAL_FILE) < 10:
        if not os.path.isfile(local_zip_filename):
            download_hydat_local(url_path)
        zipfile.ZipFile(local_zip_filename).extractall('/tmp')

def download_hydat_local(path):
    basename = os.path.basename(path)

    urllib.request.urlretrieve(os.path.join("https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www",
                                            basename),
                               os.path.join('/tmp',
                                            basename))

