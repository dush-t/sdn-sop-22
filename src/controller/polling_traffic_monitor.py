from operator import attrgetter

from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from ryu import cfg

class TrafficMonitor(simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(TrafficMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.switch_traffic_stats = {}

        CONF = cfg.CONF
        CONF.register_opts([
            cfg.FloatOpt('POLLING_INTERVAL', default=5.0, help='The interval at which switch statistics will be fetched'),
            cfg.IntOpt('TRAFFIC_THRESHOLD', default=100, help='If a switch recieves traffic higher than this, it is classified as a surge')
        ])

        self.traffic_threshold = CONF.TRAFFIC_THRESHOLD

        self.monitor_thread = hub.spawn(lambda: self._monitor(CONF.POLLING_INTERVAL))

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.datapaths:
                self.logger.info('Register datapath %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
                self.switch_traffic_stats[datapath.id] = {
                    'bytes_received': 0,
                    'packets_received': 0
                }
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info('Unregister datapath %016x', datapath.id)
                del self.datapaths[datapath.id]

    
    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(5)

    
    def _request_stats(self, datapath):
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body

        self.logger.info('datapath         '
                         'in-port  eth-dst           '
                         'out-port packets  bytes')
        self.logger.info('---------------- '
                         '-------- ----------------- '
                         '-------- -------- --------')

        switch_byte_count = 0
        switch_packet_count = 0
        
        for stat in sorted([flow for flow in body if flow.priority >= 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            self.logger.info('%016x %8x %17s %8x %8d %8d',
                             ev.msg.datapath.id,
                             stat.match['in_port'], stat.match['eth_dst'],
                             stat.instructions[0].actions[0].port,
                             stat.packet_count, stat.byte_count)
            switch_byte_count += stat.byte_count
            switch_packet_count += stat.packet_count

        bytes_received = switch_byte_count - self.switch_traffic_stats[ev.msg.datapath.id]['byte_count']
        self.logger.info('Switch %016x received %8d bytes', ev.msg.datapath.id, bytes_received)

        if bytes_received > self.traffic_threshold:
            self.logger.info('Traffic surge detected at %016x', ev.msg.datapath.id)

        self.switch_traffic_stats[ev.msg.datapath.id]['byte_count'] = switch_byte_count
        self.switch_traffic_stats[ev.msg.datapath.id]['packet_count'] = switch_packet_count
