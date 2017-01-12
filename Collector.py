import json

import time
import sys

from cocaine.services import Service

from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop

from prometheus_client.core import \
    GaugeMetricFamily, \
    CounterMetricFamily, \
    REGISTRY

from prometheus_client import start_http_server

DEFAULT_ENDPOINT = (("localhost", 10053), )

DEFAULT_QUERY_TYPE = "json"
DEFAULT_FILTER = None

class JsonMetricsRecord(object):

    CONNS_CNT_NAME = "connections"
    CONNS_REJ_NAME = "rejected"
    CONNS_ACC_NAME = "accepted"

    METER_SECTION_NAME = "meter"

    SUMMARY_NAME = "summary"

    COUNT_NAME = "count"
    M01_RATE_NAME = "m01rate"
    M05_RATE_NAME = "m05rate"
    M15_RATE_NAME = "m15rate"

    def __init__(self, name, rec, sep='_'):
        self._name = name
        self._record = rec
        self._sep = sep

    def name(self):
        return self._name

    def genMeterRecord(self, s, name, default):
        return self.genMetricsName(self.SUMMARY_NAME, name), s.get(name, default)

    def getNamedSummary(self, s):

        return [
            self.genMeterRecord(s, self.COUNT_NAME, 0),
            self.genMeterRecord(s, self.M01_RATE_NAME, 0.0),
            self.genMeterRecord(s, self.M05_RATE_NAME, 0.0),
            self.genMeterRecord(s, self.M15_RATE_NAME, 0.0)
        ]

    def getNamedMeters(self):
        r = []

        meters = self._record.get(self.METER_SECTION_NAME)

        if meters != None:
            summary = meters.get(self.SUMMARY_NAME)
            if summary != None:
                r.extend( self.getNamedSummary(summary) )

        return r

    def getNamedCounters(self):
        r = []
        r.extend( self.getNamedConnCounters() )
        return r

    def genMetricsName(self, section, name):
        return self._sep.join([ self._name, section, name])

    def genMetricsRecord(self, d, section_name, name, default = 0):
        return ( self.genMetricsName(section_name, name), d.get(name, default) )

    def getNamedConnCounters(self):
        res = []
        conns = self._record.get(self.CONNS_CNT_NAME)

        if conns == None:
            return []

        res.append( self.genMetricsRecord(conns, self.CONNS_CNT_NAME, self.CONNS_ACC_NAME))
        res.append( self.genMetricsRecord(conns, self.CONNS_CNT_NAME, self.CONNS_REJ_NAME))

        return res


class CoxxCollector(object):

    def __init__(self, endpoints, typ, query):

        self._metrics = Service('metrics', endpoints)
        self._type = typ
        self._query = query

    @coroutine
    def getMetrics(self):

        ch = yield self._metrics.fetch(self._type, self._query)
        data = yield ch.rx.get()
        raise Return(data)

    def collect(self):

        met = IOLoop.current().run_sync(self.getMetrics)

        print(json.dumps(met, indent=4, sort_keys=True))

        c = CounterMetricFamily(
            'general_counter_metrics',
            'Group of counter type metrics',
            labels=['name'])

        g = GaugeMetricFamily(
            'general_gauge_metrics',
            'Group of gauge type metrics',
            labels=['name'])

        metArr = [JsonMetricsRecord(nm, v) for nm, v in met.iteritems()]

        cntGroup = []
        meterGroup = []
        for n in metArr:
            for name, val in n.getNamedCounters():
                c.add_metric([name], val)

            for name, val in n.getNamedMeters():
                g.add_metric([name], val)

        yield c
        yield g


    def constructMeticsTable(self, met):
        finalResult = []
        for n in met:
            # print(n.name())
            # print(str(n.getNamedCounters()))
            # print(str(n.getNamedMeters()))
            finalResult.extend(n.getNamedCounters())
            finalResult.extend(n.getNamedMeters())

        return met

REGISTRY.register(
    CoxxCollector(DEFAULT_ENDPOINT, DEFAULT_QUERY_TYPE, DEFAULT_FILTER))

if __name__ == "__main__":

    print("Starting http server")
    start_http_server(9000)

    while True:
        sys.stderr.write(".")
        time.sleep(2)
