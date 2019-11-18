import logging
import json
import requests
import os.path
import geohash2
import time
from influxdb import InfluxDBClient

DEFAULT_LOGGING = 'debug'
DEFAULT_INTERVAL = 600 # in seconds
DEFAULT_RETRIES = 3 # in seconds
DEFAULT_BACKEND = 'none'

NETATMO_CLIENT_ID = None
NETATMO_CLIENT_SECRET = None
NETATMO_USERNAME = None
NETATMO_PASSWORD = None
NETATMO_AREA = None


# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #
FORMAT = ('%(asctime)-15s %(threadName)-15s '
          '%(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
logging.basicConfig(format=FORMAT, level=logging.ERROR)
log = logging.getLogger()

# --------------------------------------------------------------------------- #
# retrieve config from config.json
# --------------------------------------------------------------------------- #
main_base = os.path.dirname(__file__)
config_file = os.path.join(main_base, "config.json")
if not os.path.exists(config_file):
    raise FileNotFoundError("Configuration file not found")
with open(config_file) as f:
    # use safe_load instead load
    cfg = json.load(f)
    if 'global' in cfg:
        if 'logging' in cfg['global'] and isinstance(cfg['global']['logging'], str):
            DEFAULT_LOGGING = cfg['global']['logging']
        if 'interval' in cfg['global'] and isinstance(cfg['global']['interval'], int):
            DEFAULT_INTERVAL = cfg['global']['interval']
        if 'backend' in cfg['global'] and isinstance(cfg['global']['backend'], str):
            DEFAULT_BACKEND = cfg['global']['backend']
    if 'netatmo' in cfg:
        if isinstance(cfg['netatmo']['client_id'], str):
            NETATMO_CLIENT_ID = cfg['netatmo']['client_id']
        if isinstance(cfg['netatmo']['client_secret'], str):
            NETATMO_CLIENT_SECRET = cfg['netatmo']['client_secret']
        if isinstance(cfg['netatmo']['username'], str):
            NETATMO_USERNAME = cfg['netatmo']['username']
        if isinstance(cfg['netatmo']['password'], str):
            NETATMO_PASSWORD = cfg['netatmo']['password']
        if isinstance(cfg['netatmo']['area'], list):
            NETATMO_AREA = cfg['netatmo']['area']
    if DEFAULT_BACKEND == 'influxdb' and not 'influxdb' in cfg:
        raise Exception("Missing InfluxDB configuration")

# --------------------------------------------------------------------------- #
# set logging level
# --------------------------------------------------------------------------- #
new_logging_level = DEFAULT_LOGGING
if 'logging' in cfg:
    new_logging_level = cfg['logging']
numeric_level = getattr(logging, new_logging_level.upper())
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)
log.setLevel(numeric_level)

# --------------------------------------------------------------------------- #
# generate netatmo access token
# --------------------------------------------------------------------------- #
access_token = None;
payload = {'grant_type': 'password',
           'username': NETATMO_USERNAME,
           'password': NETATMO_PASSWORD,
           'client_id': NETATMO_CLIENT_ID,
           'client_secret': NETATMO_CLIENT_SECRET,
           'scope': ''}
try:
    log.info("Request Netatmo Access token")
    response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
    response.raise_for_status()
    access_token=response.json()["access_token"]
    refresh_token=response.json()["refresh_token"]
    scope=response.json()["scope"]
    log.debug("Access token: %s", access_token)
    log.debug("Refresh token: %s", refresh_token)
    log.debug("Scopes: %s", scope)
except requests.exceptions.HTTPError as error:
    log.error("%d %s", error.response.status_code, error.response.text)

# --------------------------------------------------------------------------- #
# request netatmo data
# --------------------------------------------------------------------------- #
params = {
    'access_token': access_token,
    'lat_ne': NETATMO_AREA[0],
    'lon_ne': NETATMO_AREA[1],
    'lat_sw': NETATMO_AREA[2],
    'lon_sw': NETATMO_AREA[3],
    'filter': 'true'
}


trial=0
data=[]
while trial < DEFAULT_RETRIES:
    trial += 1
    try:
        log.info("Request Weather data (attempt %d", trial)
        response = requests.post("https://api.netatmo.com/api/getpublicdata", params=params)
        response.raise_for_status()
        data = response.json()["body"]
        #log.debug(json.dumps(data))
        if len(data) > 0:
            log.info("%d station(s) found", len(data))
            break
        else:
            log.info("No station found, retry")
    except requests.exceptions.HTTPError as error:
        log.error("%d %s", error.response.status_code, error.response.text)
    log.info("Waiting 1 second before retrying")
    time.sleep(1)


stats={}
parsed_data = []
for station_data in data:
    measures = station_data['measures']
    coord = station_data['place']['location']
    log.debug("Coord: lon=%s lat=%s", coord[0], coord[1])
    geohash = geohash2.encode(coord[1], coord[0], 8)
    log.debug("Coord: geohash=%s", geohash)

    for device_id in measures:
        log.debug("Parsing data for device: %s", device_id)
        device_data = measures[device_id]
        if 'res' in device_data:
            log.debug("Measures found: %s", json.dumps(device_data))
            fields = device_data['type']
            log.debug("Fields: %s", fields)
            for timestamp in device_data['res']:
                if abs(int(time.time()) - int(timestamp)) > DEFAULT_INTERVAL:
                    log.debug("Data is too old: %s", timestamp)
                else:
                    point = { 
                            "measurement": "weather",
                            "tags": {
                              "geohash": geohash
                            },
                            "timestamp": int(timestamp) * 1000, # in ms
                            "fields": { }
                            }
                    for i, val in enumerate(device_data['res'][timestamp]):
                        log.debug("Working on id=%d, value=%f (type is %s)", i, val, fields[i])
                        point["fields"][fields[i]] = val
                        if not fields[i] in stats:
                            stats[fields[i]] = []
                        stats[fields[i]].append(val);
                    parsed_data.append(point);

log.debug(json.dumps(parsed_data))
for field in stats:
    log.debug("%s data: %s", field, str(stats[field]))
    log.info("Average %s = %f", field, sum(stats[field]) / float(len(stats[field])))


if DEFAULT_BACKEND == 'influxdb':
    log.info("Pushing points to InfluxDB")
    log.debug("Connecting to %s:%s with user %s on db %s", cfg['influxdb']['host'], cfg['influxdb']['port'], cfg['influxdb']['user'], cfg['influxdb']['database'])
    influx_client = InfluxDBClient(cfg['influxdb']['host'], cfg['influxdb']['port'], cfg['influxdb']['user'], cfg['influxdb']['password'], cfg['influxdb']['database'])
    influx_client.write_points(parsed_data, time_precision='ms')
    log.debug("End of InfluxDB connection")
    influx_client.close()


'''
{
    "_id": "70:ee:50:2f:05:dc",
    "mark": 14,
    "measures": {
        "02:00:00:2f:2a:d4": {
            "res": {
                "1574079580": [
                    7.7,
                    95
                ]
            },
            "type": [
                "temperature",
                "humidity"
            ]
        },
        "05:00:00:04:be:16": {
            "rain_24h": 0.404,
            "rain_60min": 0,
            "rain_live": 0,
            "rain_timeutc": 1574079586
        },
        "70:ee:50:2f:05:dc": {
            "res": {
                "1574079594": [
                    1009.5
                ]
            },
            "type": [
                "pressure"
            ]
        }
    },
    "module_types": {
        "02:00:00:2f:2a:d4": "NAModule1",
        "05:00:00:04:be:16": "NAModule3"
    },
    "modules": [
        "05:00:00:04:be:16",
        "02:00:00:2f:2a:d4"
    ],
    "place": {
        "altitude": 52,
        "city": "Mouvaux",
        "country": "FR",
        "location": [
            3.148423900000012,
            50.7042756
        ],
        "street": "All\u00e9e Ren\u00e9 Jacob",
        "timezone": "Europe/Paris"
    }
}
'''
