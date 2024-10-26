# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

from model.temperature_sensor import TemperatureSensor
import paho.mqtt.client as mqtt
import time


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

#def on_publish(mqttc, obj, mid):
    #print("mid: " + str(mid))
    #pass

# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# Configuration variables
client_id = "clientId0001-Producer"
broker_ip = "<server_ip>"
broker_port = 1883
default_topic = "sensor/temperature"
message_limit = 1000
username = "<your_username>"
password = "<your_password>"
account_topic_prefix = "/iot/user/<your_username>/"

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)
mqtt_client.on_connect = on_connect

# Set Account Username & Password
mqtt_client.username_pw_set(username, password)

# Connect to the target MQTT Broker
print("Connecting to " + broker_ip + " port: " + str(broker_port))
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

    # Create the payload string with the temperature value
    payload_string = temperature_sensor.temperature_value

    # Publish the message to the target topic composed by the account topic prefix and the default topic
    target_topic = account_topic_prefix + default_topic

    # Publish the message to the topic
    mqtt_client.publish(target_topic, payload_string)

    # Print the message sent and sleep for 1 second
    print(f"Message Sent: {message_id} Topic: {target_topic} Payload: {payload_string}")
    time.sleep(1)

mqtt_client.loop_stop()
