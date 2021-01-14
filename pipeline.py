from prefect import task, Flow, case
from prefect.agent.local import LocalAgent

import urllib.request
import requests
from bs4 import BeautifulSoup
import s3fs
import os

LocalAgent().start()


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
    return parent


@task
def extract_hydat_path(url, ext):
    # fetch reference data
    path = get_url_paths(url=url, ext=ext)[1]
    return path

@task
def verify_if_to_date(path):
    # fetch reference data
    # Wasabi cloud storage configurations
    basename = os.path.basename(path)
    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    return s3.exists(os.path.join('s3://hydat-sqlite',
                        basename))

@task
def download_hydat_file(path):
    # fetch reference data
    # Wasabi cloud storage configurations
    basename = os.path.basename(path)

    urllib.request.urlretrieve(os.path.join("https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www",
                                            basename),
                               os.path.join('/tmp',
                                            basename))

    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    s3.put(os.path.join('/tmp',
                        basename),
           os.path.join('s3://hydat-sqlite',
                        basename))
    return path


with Flow("Hydat-ETL") as flow:

    url = 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/'
    ext = 'zip'

    path = extract_hydat_path(url, ext)
    cond = verify_if_to_date(path)

    with case(cond, False):
        download_hydat_file(path)

flow.register(project_name="hydat-file-upload")

