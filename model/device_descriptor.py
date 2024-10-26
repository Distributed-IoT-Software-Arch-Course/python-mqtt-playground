import json


class DeviceDescriptor:
    """ DeviceDescriptor class contains the device information """

    def __init__(self, device_id, producer, software_version):
        """ Constructor for DeviceDescriptor class """
        self.deviceId = device_id
        self.producer = producer
        self.softwareVersion = software_version

    def to_json(self):
        """ Convert the object to a JSON string """
        return json.dumps(self, default=lambda o: o.__dict__)

