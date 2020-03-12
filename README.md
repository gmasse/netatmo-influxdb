# netatmo-influxdb

Push Netatmo public weather data into InfluxDB

## Installation
```
git clone https://github.com/gmasse/netatmo-influxdb.git
cd netatmo-influxdb
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
mv config.json.dist config.json
vi config.json
python netatmo-influxdb.py
```

## Cron
To run the script periodically, `crontab -e`
```
*/5 *   * * *       ~/netatmo-influxdb/venv/bin/python3 ~/netatmo-influxdb/netatmo-influxdb.py
```
