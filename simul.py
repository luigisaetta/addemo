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
BROKER_ADDR = "127.0.0.1"
BROKER_PORT = 1883
TOPIC_NAME = "bb/input"
CLIENT_NAME = "bb1"

FILE_NAME = "bearings_ad_test.csv"

SLEEP_TIME = 1

# for AD service
CONFIG_FILENAME = "/home/ubuntu/client/.oci/config"

SERVICE_ENDPOINT = "https://anomalydetection.aiservice.eu-frankfurt-1.oci.oraclecloud.com"
COMPARTMENT_ID = "ocid1.compartment.oc1..aaaaaaaag2cpni5qj6li5ny6ehuahhepbpveopobooayqfeudqygdtfe6h3a"

# AD Project
PROJECT_ID = "ocid1.aianomalydetectionproject.oc1.eu-frankfurt-1.amaaaaaangencdyamyo6evr4fpzed7qewqxlyohomrpnofg7a2c7ki4sn2vq"

# The trained model
SIGNAL_NAMES = ['Br11', 'Br12', 'Br21', 'Br22', 'Br31', 'Br32', 'Br41', 'Br42']
MODEL_ID = "ocid1.aianomalydetectionmodel.oc1.eu-frankfurt-1.amaaaaaangencdyakgw4bpz5ybac5s5wn7sl2hqgfjnc5dl2wcwpawmavgpa"

#
# end config
#
def on_connect(mqttc, obj, flags, connResult):

        print("Called on_connect...")

        if connResult == 0:
            connOK = True

        print("")
        if connOK == True:
            print("*** MQTT Connection  OK")
        else:
            print("*** MQTT Connection NON OK")

# Main
print('Starting simulation...')
print()

print('Loading data...')
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
mqttClient.on_connect = on_connect
mqttClient.connect(BROKER_ADDR, BROKER_PORT)

print("Connection OK...")

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

nMsgs = 0

# open the input file and then... read, publish loop
try:
    with open(FILE_NAME) as fp:
        line = fp.readline()

        while line:
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

                payloadData = []

                timestamp = datetime.strptime(msgJson['ts'], "%Y-%m-%dT%H:%M:%SZ")
                values = [float(field) for field in fields[1:]]
                print("*************")
                print(timestamp)
                print(values)
                dItem = DataItem(timestamp=timestamp, values=values)
                payloadData.append(dItem)

                print(payloadData)

                inline = InlineDetectAnomaliesRequest( model_id=MODEL_ID, request_type="INLINE", signal_names=SIGNAL_NAMES, data=payloadData)

                detect_res = ad_client.detect_anomalies(detect_anomalies_details=inline)
                print("---- Result of inference ----")
                print(detect_res.data)

                jsonStr = json.dumps(msgJson)

                # publish the msg, inorder to visualize
                try:
                    mqttClient.publish(TOPIC_NAME, jsonStr)
                
                except:
                    print('Error in sending mqtt msg...')
             
            nMsgs += 1
            time.sleep(2)

            # read next line
            line = fp.readline()

except IOError:
    print("Error: file not found: ", fName)
    print("Interrupted...")
    sys.exit(-1)

print()
print("*******************")
print("End of simulation...")
print("Num. of messages processed:", (nMsgs -1))
