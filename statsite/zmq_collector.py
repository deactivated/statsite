"""
Experimental support for ZeroMQ based collectors.
"""

import logging
import zmq

from .collector import Collector

class ZMQCollector(Collector):
    def __init__(self, endpoint="tcp://0.0.0.0:8126", direction="bind", **kwargs):
        super(ZMQCollector, self).__init__(**kwargs)

        self.logger = logging.getLogger("statsite.zmqcollector")

        self.ctx = zmq.Context()

        self.rcv_sock = self.ctx.socket(zmq.SUB)
        self.rcv_sock.setsockopt(zmq.SUBSCRIBE, "")

        if direction == "bind":
            self.rcv_sock.bind(endpoint)
        else:
            self.rcv_sock.connect(endpoint)

        self.ctl_sock = self.ctx.socket(zmq.SUB)
        self.ctl_sock.setsockopt(zmq.SUBSCRIBE, "")
        self.ctl_sock.bind("inproc://global-ctl")

        self.poller = zmq.Poller()
        self.poller.register(self.rcv_sock)
        self.poller.register(self.ctl_sock)

    def start(self):
        # Run the main server forever, blocking this thread
        self.logger.debug("ZMQCollector starting")

        ctl, rcv = self.ctl_sock, self.rcv_sock

        while True:
            socks = dict(self.poller.poll())
            if socks.get(ctl) == zmq.POLLIN:
                msg = ctl.recv()
                if msg == "END":
                    break

            if socks.get(rcv) == zmq.POLLIN:
                msg = rcv.recv()
                metrics = self._parse_metrics(msg)
                self.logger.debug(repr(metrics))
                self._add_metrics(metrics)

        self.logger.debug("ZMQCollector shutting down")

    def shutdown(self):
        self.logger.debug("ZMQCollector attempting shutdown")
        s = self.ctx.socket(zmq.PUB)
        s.connect("inproc://global-ctl")
        s.send("END")
