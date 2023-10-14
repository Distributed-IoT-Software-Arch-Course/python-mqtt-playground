# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

from model.temperature_sensor import TemperatureSensor
from model.message_descriptor import MessageDescriptor
from model.device_descriptor import DeviceDescriptor
import paho.mqtt.client as mqtt
import time
import uuid


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))


def publish_device_info():
    # MQTT Paho Publish method with all the available parameters
    # mqtt_client.publish(topic, payload=None, qos=0, retain=False)
    target_topic = "{0}/{1}/info".format(device_base_topic, device_descriptor.deviceId)
    device_payload_string = device_descriptor.to_json()
    mqtt_client.publish(target_topic, device_payload_string, 0, True)
    print(f"Device Info Published: Topic: {target_topic} Payload: {device_payload_string}")


# Configuration variables
client_id = "clientId0001-Producer"
broker_ip = "127.0.0.1"
broker_port = 1883
sensor_topic = "sensor/temperature"
device_base_topic = "device"
message_limit = 1000

mqtt_client = mqtt.Client(client_id)
mqtt_client.on_connect = on_connect

print("Connecting to " + broker_ip + " port: " + str(broker_port))
mqtt_client.connect(broker_ip, broker_port)

mqtt_client.loop_start()

# Create Demo Temperature Sensor & Device Descriptor
temperature_sensor = TemperatureSensor()
device_descriptor = DeviceDescriptor(str(uuid.uuid1()), "PYTHON-ACME_CORPORATION", "0.1-beta")

publish_device_info()

for message_id in range(message_limit):
    temperature_sensor.measure_temperature()
    payload_string = MessageDescriptor(int(time.time()),
                                       "TEMPERATURE_SENSOR",
                                       temperature_sensor.temperature_value).to_json()
    data_topic = "{0}/{1}/{2}".format(device_base_topic, device_descriptor.deviceId, sensor_topic)
    infot = mqtt_client.publish(data_topic, payload_string)
    infot.wait_for_publish()
    print(f"Message Sent: {message_id} Topic: {data_topic} Payload: {payload_string}")
    time.sleep(1)

mqtt_client.loop_stop()
