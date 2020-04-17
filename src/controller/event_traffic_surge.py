import time
from ryu.controller import event

class TrafficSurge:
    def __init__(self, surge_stats):
        self.start_timestamp = surge_stats['start_timestamp']
        self.end_timestamp = surge_stats['end_timestamp']
        self.packets_received = surge_stats['packets_received']
        self.bytes_received = surge_stats['bytes_received']
        self.datapath = surge_stats['datapath']
        self.is_new = True
        self.has_ended = False

    def extend(self, surge_stats):
        self.end_timestamp = surge_stats['end_timestamp']
        self.packets_received += surge_stats['packets_received']
        self.bytes_received += surge_stats['bytes_received']
        self.is_new = False
        self.has_ended = False

    def end(self):
        self.has_ended = True
        print("Surge ended")


class EventTrafficSurge(event.EventBase):
    """
    An event class for traffic surge at a switch.

    An instance of this class is sent to observer whenever a switch experiences a
    traffic surge.
    An instance has at least the following attributes.

    ========== ==================================================================
    Attribute  Description
    ========== ==================================================================
    timestamp  [float] the unix timestamp corresponding to the surge
    surge_data [dict] containing packet_count and byte_count
    ========== ==================================================================
    """

    def __init__(self, surge_data):
        super(EventTrafficSurge, self).__init__()
        self.timestamp = time.Time()
        self.surge_data = surge_data