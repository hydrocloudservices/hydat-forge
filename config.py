import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    # SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    # SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
    #                           'sqlite:///' + os.path.join(basedir, 'hydrodatahub.db')
    # SQLALCHEMY_DATABASE_URI_PUBLIC = os.environ.get('DATABASE_URL_PUBLIC') or \
    #                                  'sqlite:///' + os.path.join(basedir, 'hydrodatahub.db')
    # SQLALCHEMY_TRACK_MODIFICATIONS = False
    # LOG_TO_STDOUT = os.environ.get('LOG_TO_STDOUT')
    # ADMINS = ['your-email@example.com']
    # LANGUAGES = ['en']
    # REDIS_URL = os.environ.get('REDIS_URL') or 'redis://'
    # SHARABLE_DB = os.environ.get('SHARABLE_DB')
    SQLITE_HYDAT_FOLDER = os.path.join(basedir, 'hydrodatahub/database/offline_data/tmp')
    GEOJSON_BUCKET = 'https://s3.us-east-2.wasabisys.com/watersheds-polygons'

    HYDAT_URL = 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/'
    HYDAT_FORMAT = 'zip'
    HYDAT_LOCAL_FILE = "/tmp/Hydat.sqlite3"
    SQLITE_LOCAL_FILE = "sqlite:///" + os.path.join(HYDAT_LOCAL_FILE)

    # ############## PRIVATE ################
    # HQE_LECAGY_FOLDER = os.path.join(basedir, 'hydrodatahub/database/offline_data/hqe_legacy')
    # CONCAT_FILENAME = os.path.join(basedir, 'hydrodatahub/database/offline_data/concatenations.csv')