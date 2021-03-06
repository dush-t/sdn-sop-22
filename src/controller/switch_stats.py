import time
import copy
import os

from event_traffic_surge import TrafficSurge

def traffic_history_to_string(history_dict):
    return history_dict.values().join(",")

class SwitchStats:
    # Half the amount of max elements that can be in traffic_history
    traffic_history_depth = 100
    surge_history_depth = 10
    surge_threshold = 196 # in bytes/second
    polling_interval = 0.5

    def __init__(self, datapath, logpath):
        self.datapath = datapath
        self.traffic_history = []
        self.surge_history = []
        self.current_surge = None
        self.byte_count = 0
        self.packet_count = 0
        self.is_online = True

        path = logpath + str(datapath.id) + '.log'
        logfile = open(path, 'a+')
        self.logfile = logfile
    
    def add_stat(self, body):

        print('datapath         '
              'in-port  eth-dst           '
              'out-port packets  bytes')
        print('---------------- '
              '-------- ----------------- '
              '-------- -------- --------')

        switch_byte_count = 0
        switch_packet_count = 0
        
        for stat in sorted([flow for flow in body if flow.priority >= 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            print('%016x %8x %17s %8x %8d %8d',
                   self.datapath.id,
                   stat.match['in_port'], stat.match['eth_dst'],
                   stat.instructions[0].actions[0].port,
                   stat.packet_count, stat.byte_count)
            switch_byte_count += stat.byte_count
            switch_packet_count += stat.packet_count

        bytes_received = switch_byte_count - self.byte_count
        packets_received = switch_packet_count - self.packet_count
        self.traffic_history.append({
            'bytes_received': bytes_received,
            'packets_received': packets_received
        })

        self.byte_count += bytes_received
        self.packet_count += packets_received

        if len(self.traffic_history) == SwitchStats.traffic_history_depth:
            self.purge_traffic_history()
        
        result = self.check_for_surge(bytes_received, packets_received)
        return result

        

    def check_for_surge(self, bytes_received, packets_received):
        if bytes_received > SwitchStats.surge_threshold / SwitchStats.polling_interval:
            end_timestamp = time.Time()
            surge_data = {
                'datapath': self.datapath,
                'start_timestamp': end_timestamp - SwitchStats.polling_interval,
                'end_timestamp': end_timestamp ,
                'packets_received': packets_received,
                'bytes_received': bytes_received
            }
            if self.current_surge != None:
                self.current_surge.extend(surge_data)
            else:
                self.current_surge = TrafficSurge(surge_data)

            return self.current_surge
        else:
            if self.current_surge != None:
                self.current_surge.end()
                surge = copy.copy(self.current_surge)
                self.current_surge = None
                return surge
            else:
                return None


    # Called when length of traffic_history list crosses a threshold
    # value. This function will purge the list to a file or something.
    def purge_traffic_history(self):
        values_to_write = list(map(traffic_history_to_string, self.traffic_history))
        self.logfile.writelines(values_to_write)
        self.traffic_history = []

    
    def mark_offline(self):
        self.is_online = False
        self.logfile.close()

    def mark_online(self):
        self.is_online = True
        self.logfile = open(self.logfile.name, "a+")