from dto.action_descriptor import ActionDescriptor
from dto.event_descriptor import EventDescriptor
from model.switch_actuator import SwitchActuator
from model.temperature_sensor import TemperatureSensor
from dto.message_descriptor import MessageDescriptor
from dto.device_descriptor import DeviceDescriptor
import paho.mqtt.client as mqtt
import time
import threading
import json

# Configuration variables
client_id = "clientId0001-Producer"
device_id = "device001"
broker_ip = "127.0.0.1"
broker_port = 1883
temperature_telemetry_topic = "telemetry/temperature"
switch_telemetry_topic = "telemetry/switch"
event_topic = "event"
action_topic_subscribe_filter = f'device/{device_id}/action/switch'
device_base_topic = "device"
message_limit = 1000
TEMPERATURE_ALERT_LIMIT = 35

# Initialize the Temperature Sensor, Actuator and Device Descriptor
temperature_sensor = TemperatureSensor()
switch_actuator = SwitchActuator()

# Create a Device Descriptor with the device id
device_descriptor = DeviceDescriptor(device_id, "PYTHON-ACME_CORPORATION", "0.1-beta")

# Global Variables for the MQTT Client, Device Thread and Publishing Status
mqtt_client = None
device_thread = None

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    global device_thread

    # Print the connection result contained in the variable rc
    print("Connected with result code " + str(rc))

    # Check the connection result
    if rc == 0:
        print("Connection successful !")

        # Start device behaviour in a separate thread
        # This is due to the fact that the on_connect method is called in the main thread
        # that is also used by the MQTT Client to process the incoming messages through the loop_forever method
        device_thread = threading.Thread(target=device_behaviour)
        device_thread.start()
    else:
        print(f'Connection failed with result code {str(rc)}')

# Define a callback method to receive asynchronous messages
def on_message(client, userdata, message):

    # If the message received is in the device info topic
    # We use the MQTT Paho topic_matches_sub method to match the topic with the device info topic
    if mqtt.topic_matches_sub(action_topic_subscribe_filter, message.topic):
        handle_device_action_topic(message)
    # If the message received is in an unmanaged topic
    else:
        print("Unmanaged Topic !")

def handle_device_action_topic(message):
    """
    Handle the device action message received from the broker
    :param message:
    :return:
    """
    global switch_actuator

    try:
        # Decode the message payload from an array of bytes to a string
        message_payload = str(message.payload.decode("utf-8"))

        # Create a DeviceDescriptor object from the JSON payload
        action_descriptor = ActionDescriptor(**json.loads(message_payload))

        # Check the action type and action value
        if action_descriptor.action_type == "SWITCH" and action_descriptor.action_value == "ON" and not switch_actuator.switch_status:
            switch_actuator.set_switch_status(True)
            print(f"Action Received: {action_descriptor.action_type} Value: {action_descriptor.action_value}")
            publish_switch_telemetry()
        elif action_descriptor.action_type == "SWITCH" and action_descriptor.action_value == "OFF" and switch_actuator.switch_status:
            switch_actuator.set_switch_status(False)
            print(f"Action Received: {action_descriptor.action_type} Value: {action_descriptor.action_value}")
            publish_switch_telemetry()
        else:
            print(f"Unmanaged Action Received: {action_descriptor.action_type} Value: {action_descriptor.action_value}")
    except Exception as e:

        # Print the exception traceback
        #traceback.print_exc()

        # Print the error message
        print(f"Error processing message: {e}")

def device_behaviour():
    """Device Behaviour: Publishes the Device Info and Telemetry Data"""

    # Subscribe to the action topic
    mqtt_client.subscribe(action_topic_subscribe_filter)

    # Publish the device info
    publish_device_info()

    # Publish the switch telemetry (the switch is ON by default)
    publish_switch_telemetry()

    # Start the telemetry publishing
    start_telemetry_publishing()


def start_telemetry_publishing():
    """Publishes the temperature sensor data to the broker"""

    try:
        print("Starting Telemetry Publishing ...")

        # Publish messages with the temperature value
        for message_id in range(message_limit):

            # Measure the temperature
            temperature_sensor.measure_temperature()

            # Send the message only if the publishing status is True
            if switch_actuator.switch_status:

                # Create the payload String in JSON format with the temperature value
                payload_string = MessageDescriptor(int(time.time()),
                                                   "TEMPERATURE_SENSOR",
                                                   temperature_sensor.temperature_value).to_json()

                # Build the target topic
                data_topic = "{0}/{1}/{2}".format(device_base_topic, device_descriptor.device_id, temperature_telemetry_topic)

                # Publish the message to the target topic
                mqtt_client.publish(data_topic, payload_string)

                # Print the message sent
                print(f"Message Sent: {message_id} Topic: {data_topic} Payload: {payload_string}")

                # If temperature is above 40 send an OVER_HEATING Event
                if temperature_sensor.temperature_value > TEMPERATURE_ALERT_LIMIT:

                    print("Temperature is above 40 degrees !")

                    # Create the payload String in JSON format with the temperature value
                    payload_string = EventDescriptor(int(time.time()),
                                                       "OVER_HEATING",
                                                       temperature_sensor.temperature_value).to_json()

                    # Build the target topic
                    target_event_topic = "{0}/{1}/{2}".format(device_base_topic, device_descriptor.device_id, event_topic)

                    # Publish the message to the target topic
                    mqtt_client.publish(target_event_topic, payload_string)

                    # Print the message sent
                    print(f"Event Sent: Topic: {target_event_topic} Payload: {payload_string}")

            # Sleep for 1 second
            time.sleep(1)

    except Exception as e:
        print(f"Error starting Telemetry Publishing: {str(e)}")

def publish_switch_telemetry():
    """ Publishes the switch telemetry data to the broker """

    try:
        print("Publishing Switch Telemetry ...")

        switch_value = "ON" if switch_actuator.switch_status else "OFF"

        # Create the payload String in JSON format with the switch value
        payload_string = MessageDescriptor(int(time.time()),
                                        "SWITCH",
                                        switch_value).to_json()

        # Build the target topic
        data_topic = "{0}/{1}/{2}".format(device_base_topic, device_descriptor.device_id, switch_telemetry_topic)

        # Publish the message to the target topic
        mqtt_client.publish(data_topic, payload_string)

        # Print the message sent
        print(f"Message Sent Topic: {data_topic} Payload: {payload_string}")

    except Exception as e:
        print(f"Error publishing Switch Telemetry: {str(e)}")

def publish_device_info():
    """Publishes the device descriptor to the broker"""

    try:
        print("Publishing Device Info ...")

        # Build the target topic and the payload string
        target_topic = "{0}/{1}/info".format(device_base_topic, device_descriptor.device_id)

        # Serialize the Device Descriptor to a JSON string
        device_payload_string = device_descriptor.to_json()

        # Publish the Device Descriptor to the target topic
        mqtt_client.publish(target_topic, device_payload_string, 0, True)

        # Print the Device Info Published
        print(f"Device Info Published: Topic: {target_topic} Payload: {device_payload_string}")
    except Exception as e:
        print(f"Error publishing Device Info: {str(e)}")

# Main Script
if __name__ == "__main__":

    # Create a new MQTT Client
    mqtt_client = mqtt.Client(client_id)

    # Attach Paho OnMessage Callback Method
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to the target MQTT Broker
    print("Connecting to " + broker_ip + " port: " + str(broker_port))
    mqtt_client.connect(broker_ip, broker_port)

    # Start the MQTT Client with the loop_forever method to process the callbacks in a blocking way
    mqtt_client.loop_forever()
