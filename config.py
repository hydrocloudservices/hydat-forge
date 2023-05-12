from datetime import datetime, timedelta
import os


class Config(object):

    CLIENT_KWARGS = {'endpoint_url': 'https://s3.us-east-1.wasabisys.com',
                     'region_name': 'us-east-1'}
    CONFIG_KWARGS = {'max_pool_connections': 30}
    PROFILE = 'default'
    STORAGE_OPTIONS = {'profile': PROFILE,
                       'client_kwargs': CLIENT_KWARGS,
                       'config_kwargs': CONFIG_KWARGS
                       }

   
<<<<<<< HEAD
    HYDAT_ZARR_BUCKET = f"hydrometric/source/hydat/zarr/{datetime.now().strftime('%Y-%m-%d')}/"
=======
    DEH_ZARR_BUCKET = f"hydrometric/source/deh/test/{datetime.now().strftime('%Y-%m-%d')}"
>>>>>>> edefc0b66de9da40d8f5799b99f8255f51b9db77
