# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

import paho.mqtt.client as mqtt
from model.message_descriptor import MessageDescriptor
import json
import traceback

# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

    # Print the connection result contained in the variable rc
    print("Connected with result code " + str(rc))

    # After the connection is established, we subscribe to the default topic
    mqtt_client.subscribe(default_topic)

    # Print the topic we are subscribed to
    print("Subscribed to: " + default_topic)


# Define a callback method to receive asynchronous messages
def on_message(client, userdata, message):

    # If the message received is in the default topic, we print the message
    if message.topic == default_topic:
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
            traceback.print_exc()

            # Print the error message
            print(f"Error processing message: {e}")

# Configuration variables
client_id = "clientId0001-Consumer"
broker_ip = "127.0.0.1"
broker_port = 1883
default_topic = "device/temperature"
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
