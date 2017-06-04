from configparser import RawConfigParser

config = RawConfigParser()
config.read('../config.properties')

DATA_LOCATION = config.get('DATA', 'location')