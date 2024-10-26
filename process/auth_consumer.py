# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt
import time

import paho.mqtt.client as mqtt

# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    # Print the connection result contained in the variable rc
    print("Connected with result code " + str(rc))

    # After the connection is established, we subscribe to the target topic composed by the account
    # topic prefix and the wildcard character #
    target_topic = account_topic_prefix + "#"

    # Subscribe to the target topic
    mqtt_client.subscribe(target_topic)

    # Print the topic we are subscribed to
    print("Subscribed to: " + target_topic)


# Define a callback method to receive asynchronous messages
def on_message(client, userdata, message):
    # Print the message received from the broker
    # Available attributes: payload, topic, qos, retain
    print("\n##########################################################")
    print("message received: ", str(message.payload.decode("utf-8")))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)
    print("##########################################################")


# Configuration variables
client_id = "clientId0001-Consumer"
broker_ip = "<server_ip>"
broker_port = 1883
default_topic = "device/temperature"
message_limit = 1000
username = "<your_username>"
password = "<your_password>"
account_topic_prefix = "/iot/user/<your_username>/"

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)

# Attack Paho OnMessage Callback Method
mqtt_client.on_message = on_message
mqtt_client.on_connect = on_connect

# Set Account Username & Password
mqtt_client.username_pw_set(username, password)

# Connect to the target MQTT Broker
mqtt_client.connect(broker_ip, broker_port)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqtt_client.loop_forever()
