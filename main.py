import http.client
import json
import time
import os
import requests
from influxdb import InfluxDBClient
import schedule
from pprint import pprint

#LOAD .env locals
# if os.path.exists('.env'):
#     from dotenv import load_dotenv
#     load_dotenv()

#GLOBALS
LIVE_CONN = bool(os.environ.get('LIVE_CONN',''))
INFLUX_HOST = os.environ.get('INFLUX_HOST','')
INFLUX_HOST_PORT = int(os.environ.get('INFLUX_HOST_PORT',''))
INFLUX_DATABASE = os.environ.get('INFLUX_DATABASE','')
RUNMINS =  int(os.environ.get('RUNMINS',''))
URL = os.environ.get('URL','')
MEASUREMENT = os.environ.get('MEASUREMENT','')
TAG = os.environ.get('TAG','')
IN1 = os.environ.get('IN1','')
IN2 = os.environ.get('IN2','')
JSON_OUTPUT = 'output.json'

INFLUX_CLIENT = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_HOST_PORT, database=INFLUX_DATABASE)

# Get json data
def get_json(*args):
    resp = requests.get(URL)
    payload_data = resp.json()
    with open(JSON_OUTPUT, 'w') as outfile:
        json.dump(payload_data, outfile)

def get_saved_data(*args):
    if LIVE_CONN == True:
        get_json()

    with open(JSON_OUTPUT) as json_file:
        working_data = json.load(json_file)
    return working_data

def write_to_influx(data_payload):
    INFLUX_CLIENT.write_points(data_payload)
     

def sort_json(working_data):
    # Interate over payload and pull out data points
    pprint(working_data)
    print('#'*30)
    
    # Base info for insert
    base_dict = {'measurement' : MEASUREMENT, 'tags' : {'name': TAG}}
    time_stamp = working_data ['time']['updatedISO']
    base_dict.update({'time': time_stamp})

    # Fields logic for insert
    fields_data = {}
    data_points = working_data [IN1]
    for k,v in data_points.items():
        fields_data.update({k : v[IN2]})
    base_dict.update({'fields' : fields_data})
    pprint(base_dict)

    # Construct payload and insert
    data_payload = [base_dict] 
    print("SUBMIT:" + str(data_payload))
    print('#'*30) 
    write_to_influx(data_payload)

def do_it():
    working_data = get_saved_data()
    sort_json(working_data)

def main():
    ''' Main entry point of the app '''
    do_it()
    schedule.every(RUNMINS).minutes.do(do_it)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    ''' This is executed when run from the command line '''
    main()