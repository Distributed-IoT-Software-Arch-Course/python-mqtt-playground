# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

from model.temperature_sensor import TemperatureSensor
from dto.message_descriptor import MessageDescriptor
import paho.mqtt.client as mqtt
import time

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)


# Configuration variables
client_id = "clientId0001-Producer"
broker_ip = "127.0.0.1"
broker_port = 1883
default_topic = "device/temperature"
message_limit = 1000

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)

# Attach Paho OnMessage Callback Method
mqtt_client.on_connect = on_connect

# Connect to the target MQTT Broker
print("Connecting to "+ broker_ip + " port: " + str(broker_port))
mqtt_client.connect(broker_ip, broker_port)

# Start the MQTT Client with the loop_start method to process the callbacks in a separate thread
mqtt_client.loop_start()

# Create Demo Temperature Sensor
temperature_sensor = TemperatureSensor()

# MQTT Paho Publish method with all the available parameters
# mqtt_client.publish(topic, payload=None, qos=0, retain=False)

# Publish messages with the temperature value
for message_id in range(message_limit):

    # Measure the temperature
    temperature_sensor.measure_temperature()

    # Create the payload String in JSON format with the temperature value
    payload_string = MessageDescriptor(int(time.time()),
                                       "TEMPERATURE_SENSOR",
                                       temperature_sensor.temperature_value).to_json()

    # Publish the message to the default topic
    mqtt_client.publish(default_topic, payload_string)

    # Print the message sent
    print(f"Message Sent: {message_id} Topic: {default_topic} Payload: {payload_string}")

    # Wait for 1 second before sending the next message
    time.sleep(1)

# Stop the MQTT Client with the loop_stop method in order to stop the processing of the callbacks
mqtt_client.loop_stop()
