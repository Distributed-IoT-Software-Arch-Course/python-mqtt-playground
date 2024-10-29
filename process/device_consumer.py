# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

import paho.mqtt.client as mqtt
from dto.device_descriptor import DeviceDescriptor
from dto.message_descriptor import MessageDescriptor
import json


# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

    # Print the connection result contained in the variable rc
    print("Connected with result code " + str(rc))

    # After the connection is established, subscribe to the device info and data topics
    mqtt_client.subscribe(device_info_topic)
    mqtt_client.subscribe(data_topic)

    # Print the topics we are subscribed to
    print("Subscribed to: " + device_info_topic)
    print("Subscribed to: " + data_topic)


# Define a callback method to receive asynchronous messages
def on_message(client, userdata, message):

    # If the message received is in the device info topic
    # We use the MQTT Paho topic_matches_sub method to match the topic with the device info topic
    if mqtt.topic_matches_sub(device_info_topic, message.topic):
        handle_device_info_message(message)
    # If the message received is in the data topic
    # We use the MQTT Paho topic_matches_sub method to match the topic with the device info topic
    elif mqtt.topic_matches_sub(data_topic, message.topic):
        handle_device_telemetry_message(message)
    # If the message received is in an unmanaged topic
    else:
        print("Unmanaged Topic !")


def handle_device_info_message(message):
    """
    Handle the device info message received from the broker
    :param message:
    :return:
    """

    try:
        # Decode the message payload from an array of bytes to a string
        message_payload = str(message.payload.decode("utf-8"))

        # Create a DeviceDescriptor object from the JSON payload
        device_descriptor = DeviceDescriptor(**json.loads(message_payload))

        # Print the message received from the broker
        print(f"Received IoT Message (Retained:{message.retain}): Topic: {message.topic} DeviceId: {device_descriptor.device_id} Manufacturer: {device_descriptor.producer} SoftwareVersion: {device_descriptor.software_version}")
    except Exception as e:

        # Print the exception traceback
        #traceback.print_exc()

        # Print the error message
        print(f"Error processing message: {e}")


def handle_device_telemetry_message(message):
    """
    Handle the device telemetry message received from the broker
    :param message:
    :return:
    """
    try:
        # Decode the message payload from an array of bytes to a string
        message_payload = str(message.payload.decode("utf-8"))

        # Create a MessageDescriptor object from the JSON payload
        message_descriptor = MessageDescriptor(**json.loads(message_payload))

        # Print the message received from the broker
        print(
            f"Received IoT Message: Topic: {message.topic} Timestamp: {message_descriptor.timestamp} Type: {message_descriptor.value_type} Value: {message_descriptor.value}")
    except Exception as e:

        # Print the exception traceback
        #traceback.print_exc()

        # Print the error message
        print(f"Error processing message: {e}")


# Configuration variables
client_id = "clientId0001-Consumer"
broker_ip = "127.0.0.1"
broker_port = 1883
device_info_topic = "device/+/info"
data_topic = "device/+/sensor/#"
message_limit = 1000

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)

# Attack Paho OnMessage Callback Method
mqtt_client.on_message = on_message
mqtt_client.on_connect = on_connect

# Connect to the target MQTT Broker
mqtt_client.connect(broker_ip, broker_port)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqtt_client.loop_forever()
