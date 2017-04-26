import requests
import datetime
import json
import time



url_sensor = "https://www.data199.com/api/pv1/device/lastmeasurement"

payload_sensor = "deviceids=0844F59172CD%2C0B250836A75B&phoneid=789807439177"
headers_sensor = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache",
    #    'postman-token': "3c48dde2-14ca-413f-6574-6222fa5eb304"
}
headers_sink = {
    'content-type': "application/json",
    'cache-control': "no-cache",
}
last_rain = {}
last_wind = {}
id = 0
last_rain['id']=0
last_rain['amount']=0
last_wind['id']=0

print("Starting")
while True:
    time.sleep(25)

    response = requests.request("POST", url_sensor, data=payload_sensor, headers=headers_sensor).json()

    if response['success'] != True:
        print ("problem contacting the cloud")
        exit(1)

    for device in response['devices']:
        measurement = device['measurement']
        id = measurement['idx']
        m_dict = {}
        m_dict['ts_measurement'] = datetime.datetime.fromtimestamp(measurement['ts']).isoformat()
        m_dict['ts_lastseen'] = datetime.datetime.fromtimestamp(measurement['c']).isoformat()
        if 'r' in measurement:
            # rain sensor
            if last_rain['id'] == id:
                print("rain: same ID, nothing to do")
                print(m_dict)
                continue
            type = device['deviceid']
            m_dict['total_rain'] = measurement['r']
            m_dict['temperature'] = measurement['t1']
            last_rain['id'] = id
            m_dict['additional_rain'] = m_dict['total_rain'] - last_rain['amount']
            last_rain['amount'] = m_dict['total_rain']
        elif 'ws' in measurement:
            # wind sensor
            if last_wind['id'] == id:
                print("wind: same ID, nothing to do")
                print(m_dict)
                continue
            type = device['deviceid']
            m_dict['windspeed'] = measurement['ws']
            m_dict['windgust'] = measurement['wg']
            m_dict['winddir'] = measurement['wd']
            last_wind['id'] = id
        else :
            type = 'unknown'

        payload_sink = json.dumps(m_dict, ensure_ascii=False)
        query = "http://docker.moik.org:9200/meteosensor/" + type + "/" + str(id)
        print(query)
        print(payload_sink)

        # put to ES

        response = requests.put(query, data=payload_sink, headers=headers_sink)
    #    response = requests.get('http://docker.moik.org:9200/meteosensor/_search', headers=headers)
        if response.ok == True:
            print ("successfully inserted into ES")
        else:
            print ("not added to ES")

    print ("finished.... sleeping")