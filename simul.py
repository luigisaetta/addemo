import json
import time
from datetime import datetime
import sys
import paho.mqtt.client as mqtt

import oci
from oci.config import from_file
from oci.ai_anomaly_detection.anomaly_detection_client import AnomalyDetectionClient
from oci.ai_anomaly_detection.models.model_training_details import ModelTrainingDetails

from oci.ai_anomaly_detection.models.data_item import DataItem
from oci.ai_anomaly_detection.models.inline_detect_anomalies_request import InlineDetectAnomaliesRequest


#
# config
#
DEBUG = False

BROKER_ADDR = "138.3.246.176"
BROKER_PORT = 1883
TOPIC_SIGNALS = "bb/input"
TOPIC_ANOMALIES = "bb/anomalies"

WINDOW_SIZE = 30

CLIENT_NAME = "bb1"

FILE_NAME = "bearings_ad_test.csv"

SLEEP_TIME = 1

# for AD service
CONFIG_FILENAME = "/Users/lsaetta/.oci/config"

SERVICE_ENDPOINT = "https://anomalydetection.aiservice.eu-frankfurt-1.oci.oraclecloud.com"
COMPARTMENT_ID = "ocid1.compartment.oc1..aaaaaaaag2cpni5qj6li5ny6ehuahhepbpveopobooayqfeudqygdtfe6h3a"

# AD Project
PROJECT_ID = "ocid1.aianomalydetectionproject.oc1.eu-frankfurt-1.amaaaaaangencdyamyo6evr4fpzed7qewqxlyohomrpnofg7a2c7ki4sn2vq"

# The trained model
SIGNAL_NAMES = ['Br11', 'Br12', 'Br21', 'Br22', 'Br31', 'Br32', 'Br41', 'Br42']
MODEL_ID = "ocid1.aianomalydetectionmodel.oc1.eu-frankfurt-1.amaaaaaangencdyakgw4bpz5ybac5s5wn7sl2hqgfjnc5dl2wcwpawmavgpa"

YEAR = "2021"

#
# end config
#
def compute_timestamp(str_date):
    # NodeRed chart require it in msec
    ts = time.mktime(datetime.strptime(str_date, "%d/%m/%Y %H").timetuple()) * 1000
    return ts 

# Main
print('Starting simulation...')
print()

print(f'OCI SDK version: {oci.__version__}')
print()

print('Loading OCI config...')
# read OCI config from config file
config = from_file(CONFIG_FILENAME)

print(config)
print()

# connect to MQTT broker
mqttClient = mqtt.Client(CLIENT_NAME, protocol=mqtt.MQTTv311)
mqttClient.connect(BROKER_ADDR, BROKER_PORT)

print("MQTT Connection OK...")
print()

# creating AD client
ad_client = AnomalyDetectionClient(config, service_endpoint=SERVICE_ENDPOINT)

#
# Get the information on the AD project
#
ad_proj = ad_client.get_project(project_id=PROJECT_ID)

print("----Information on the AD Project---")
print(ad_proj.data)
print()

#
# Get the information on the Trained model
#
ad_model = ad_client.get_model(model_id=MODEL_ID)

print("----Information on the AD Model---")
print(ad_model.data)
print()

print('Loading data...')
print()

nTotalAnomalies = 0
nMsgs = 0
nMessInWindow = 0

# open the input file and then... read, publish loop
try:
    with open(FILE_NAME) as fp:
        line = fp.readline()

        # initialize the WINDOW
        payloadData = []

        while line:
            if DEBUG:
                print(line)
            
            # skip header            
            if nMsgs > 0:            
                fields = line.split(",")

                #
                # simulate reading from sensors
                #
                msgJson = {}
                # timestamp in ISO 8601 format
                msgJson['ts'] = fields[0]
                msgJson['br11'] = float(fields[1])
                msgJson['br12'] = float(fields[2])
                msgJson['br21'] = float(fields[3])
                msgJson['br22'] = float(fields[4])
                msgJson['br31'] = float(fields[5])
                msgJson['br32'] = float(fields[6])
                msgJson['br41'] = float(fields[7])
                msgJson['br42'] = float(fields[8])
                
                # prepare data for call to AD service
                # the batch is put in payloadData
                timestamp = datetime.strptime(msgJson['ts'], "%Y-%m-%dT%H:%M:%SZ")
                values = [float(field) for field in fields[1:]]
                
                dItem = DataItem(timestamp=timestamp, values=values)
                payloadData.append(dItem)
                nMessInWindow += 1

                if nMessInWindow == WINDOW_SIZE:
                    # ready, call the service
                    inline = InlineDetectAnomaliesRequest( model_id=MODEL_ID, request_type="INLINE", signal_names=SIGNAL_NAMES, data=payloadData)

                    detect_res = ad_client.detect_anomalies(detect_anomalies_details=inline)
                    
                    print("---- Result of inference ----")
                    print(detect_res.data)

                    nTotalAnomalies += len(detect_res.data.detection_results)
                    
                    try:
                        msgAnom = {}
                        # get date and hour from timestamp
                        strDate = msgJson['ts'][5:10]
                        strDay = strDate[3:5]
                        strMonth = strDate[0:2]
                        strHour = msgJson['ts'][11:13] 
                        strDateHour = strDay + "/" + strMonth + "/" + YEAR + " " + strHour
                        msgAnom['ts'] = strDateHour
                        msgAnom['tts'] = compute_timestamp(strDateHour)
                        msgAnom['total'] = nTotalAnomalies
                        msgAnomStr = json.dumps(msgAnom)

                        mqttClient.publish(TOPIC_ANOMALIES, msgAnomStr)
                
                    except Exception as e:
                        print('1-Error in sending mqtt msg...')
                        print(e)

                    # after: empty the WINDOW
                    payloadData = []
                    nMessInWindow = 0 

                    # to slow down a bit the simulation
                    time.sleep(SLEEP_TIME)

                jsonStr = json.dumps(msgJson)

                # publish the msg, in order to visualize readings
                try:
                    mqttClient.publish(TOPIC_SIGNALS, jsonStr)
                
                except Exception as e:
                    print('2-Error in sending mqtt msg...')
                    print(e)
             
            nMsgs += 1

            # read next line
            line = fp.readline()

except IOError:
    print("Error: file not found: ", FILE_NAME)
    print("Interrupted...")
    sys.exit(-1)

print()
print("*******************")
print("End of simulation...")
print("Num. of messages processed:", (nMsgs -1))
