from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu import cfg

from .switch_stats import SwitchStats
from .event_traffic_surge import EventTrafficSurgeStart, EventTrafficSurgeEnd


class SurgeMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SurgeMonitor, self).__init__(*args, **kwargs)
        self.switches = {}

        CONF = cfg.CONF
        CONF.register_opts([
            cfg.FloatOpt('MAX_POLLING_INTERVAL', default=5.0, help='The interval at which switch statistics will be fetched'),
            cfg.IntOpt('TRAFFIC_THRESHOLD', default=100, help='If a switch recieves traffic higher than this, it is classified as a surge'),
        ])
        self.conf = CONF



    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.switches:
                self.switches[datapath.id] = SwitchStats(datapath)
            else:
                (self.swithes[datapath.id]).mark_online()
        elif ev.state == DEAD_DISPATCHER:
            (self.switches[datapath.id]).mark_offline()


    
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        switch = self.switches[ev.msg.datapath.id]
        surge = switch.add_stat(body)

        if surge != None:
            if surge.is_new:
                # Send EventTrafficSurgeStart message
                ev = EventTrafficSurgeStart(surge)
                self.send_event_to_observers(ev)
            elif surge.has_ended:
                # Send EventTrafficSurgeEnd message
                ev = EventTrafficSurgeEnd(surge)
                self.send_event_to_observers(ev)


    def _monitor(self, polling_interval):
        while True:
            for dp in self.switches.values():
                ofproto = dp.ofproto
                parser = dp.ofproto_parser
                req = parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(polling_interval)
