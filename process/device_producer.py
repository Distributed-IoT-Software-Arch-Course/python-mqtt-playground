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
    # Print the connection result contained in the variable rc
    print("Connected with result code " + str(rc))


def publish_device_info():
    # MQTT Paho Publish method with all the available parameters
    # mqtt_client.publish(topic, payload=None, qos=0, retain=False)

    # Build the target topic and the payload string
    target_topic = "{0}/{1}/info".format(device_base_topic, device_descriptor.deviceId)

    # Serialize the Device Descriptor to a JSON string
    device_payload_string = device_descriptor.to_json()

    # Publish the Device Descriptor to the target topic
    mqtt_client.publish(target_topic, device_payload_string, 0, True)

    # Print the Device Info Published
    print(f"Device Info Published: Topic: {target_topic} Payload: {device_payload_string}")


# Configuration variables
client_id = "clientId0001-Producer"
broker_ip = "127.0.0.1"
broker_port = 1883
sensor_topic = "sensor/temperature"
device_base_topic = "device"
message_limit = 1000

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)

# Attach Paho OnMessage Callback Method
mqtt_client.on_connect = on_connect

# Connect to the target MQTT Broker
print("Connecting to " + broker_ip + " port: " + str(broker_port))
mqtt_client.connect(broker_ip, broker_port)

# Start the MQTT Client with the loop_start method to process the callbacks in a separate thread
mqtt_client.loop_start()

# Create Demo Temperature Sensor & Device Descriptor
temperature_sensor = TemperatureSensor()

# Create a Device Descriptor with a random UUID
device_descriptor = DeviceDescriptor(str(uuid.uuid1()), "PYTHON-ACME_CORPORATION", "0.1-beta")

# Publish the Device Info with the dedicated method
publish_device_info()

# Publish messages with the temperature value
for message_id in range(message_limit):

    # Measure the temperature
    temperature_sensor.measure_temperature()

    # Create the payload String in JSON format with the temperature value
    payload_string = MessageDescriptor(int(time.time()),
                                       "TEMPERATURE_SENSOR",
                                       temperature_sensor.temperature_value).to_json()

    # Build the target topic
    data_topic = "{0}/{1}/{2}".format(device_base_topic, device_descriptor.deviceId, sensor_topic)

    # Publish the message to the target topic
    mqtt_client.publish(data_topic, payload_string)

    # Print the message sent
    print(f"Message Sent: {message_id} Topic: {data_topic} Payload: {payload_string}")
    time.sleep(1)

# Stop the MQTT Client with the loop_stop method in order to stop the processing of the callbacks
mqtt_client.loop_stop()
