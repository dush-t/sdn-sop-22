import time
import copy
from controller.event_traffic_surge import TrafficSurge


class SwitchStats:
    # Half the amount of max elements that can be in traffic_history
    traffic_history_depth = 100
    surge_history_depth = 10
    surge_threshold = 98 # in bytes/second
    polling_interval = 0.5

    def __init__(self, datapath):
        self.datapath = datapath
        
        self.traffic_history = []
        self.surge_history = []
        
        self.current_surge = None

        self.byte_count = 0
        self.packet_count = 0
        self.polling_interval = 10
    
    def add_stat(self, stat):
        bytes_received = stat.byte_count - self.byte_count
        packets_received = stat.packet_count - self.packet_count
        self.traffic_history.append({
            'bytes_received': bytes_received,
            'packets_received': packets_received
        })

        self.byte_count += stat.byte_count
        self.packet_count += stat.packet_count

        if len(self.traffic_history) == traffic_history_depth:
            self.purge_traffic_history()
        
        result = check_for_surge(bytes_received, packets_received)
        return result

        

    def check_for_surge(self, bytes_received, packets_received):
        if bytes_received > surge_threshold / self.polling_interval:
            end_timestamp = time.Time()
            surge_data = {
                'datapath': self.datapath,
                'start_timestamp': end_timestamp - polling_interval,
                'end_timestamp': end_timestamp ,
                'packets_received': packets_received,
                'bytes_received': bytes_received
            }
            if self.ongoing_surge != None:
                self.ongoing_surge.extend(surge_data)
            else:
                self.ongoing_surge = TrafficSurge(surge_data)

            return self.ongoing_surge
        else:
            if self.ongoing_surge != None:
                self.ongoing_surge.end()
                surge = copy.copy(self.ongoing_surge)
                self.ongoing_surge = None
                return surge
            else:
                return None


    # Called when length of traffic_history list crosses a threshold
    # value. This function will purge the list to a file or something.
    def purge_traffic_history(self):
        break_point = int(traffic_history_depth/2)
        old_values = traffic_history[0:break_point]
        pass


