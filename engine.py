import logging
import random
import time
import traceback

from confluent_kafka.cimpl import TopicPartition
from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.internet import task
from twisted.internet import threads
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web.client import Agent, readBody, ProxyAgent, RedirectAgent
from twisted.web.http_headers import Headers

import kspifka.kafka_default_settings as defaults
from kafka_scrapy.connection import get_redis
from kspifka.utils import SettingsWrapper
from . import kafka_default_settings as default
from .scheduler import Scheduler


class Slot:

    def __init__(self, nextcall):
        self.nextcall = nextcall
        self.heartbeat = task.LoopingCall(nextcall)

    def close(self):
        self.closing = defer.Deferred()
        self._maybe_fire_closing()
        return self.closing

    def _maybe_fire_closing(self):
        if self.closing:
            if self.heartbeat.running:
                self.heartbeat.stop()
            self.closing.callback(None)


class engine(object):
    def __init__(self, topic=None, scheduler=Scheduler, settings=None,
                 ip_ext=False, headers=None, ext=False, db_type=None, *args, **kwargs):
        super(engine, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger()
        self.topic = topic
        self._scheduler = scheduler
        self.slot = None
        self.settings = SettingsWrapper().load(default=settings)
        self.start_time = time.time()
        self.consume_offset = 0
        self.consumers_offset = []
        self.ip_ext = ip_ext
        self.check_times = 0
        self.ban_request = []
        self.ip_pool = []
        self.headers = headers
        self.ip = None
        self.ext = ext
        self.crawl_queue = set([])
        self.logger = logging.getLogger(__name__)
        self.db_type = db_type

    def set_up(self):
        if self.db_type:
            conn_kwargs = self.settings.get('conn_kwargs', default.conn_kwargs)
            self.dbpool = adbapi.ConnectionPool(self.db_type,
                                                charset=self.settings.get('CHARSET', 'utf8'),
                                                use_unicode=self.settings.get('USE_UNICODE', True),
                                                connect_timeout=self.settings.get('CONNECT_TIMEOUT', 5),
                                                **conn_kwargs)
        self.server = get_redis(self.settings)
        self.scheduler = self._scheduler(self.server, self.settings)
        self.scheduler.open()

    @defer.inlineCallbacks
    def multithreading_fun(self, *args):
        out = defer.Deferred()
        threads.deferToThread(self.fun, *args).addCallback(self.parse)
        yield out

    def check(self):
        if isinstance(self.scheduler.queue.partition, int):
            self._consume_offset = self.scheduler.queue.consumer.get_watermark_offsets(
                partition=TopicPartition(topic=defaults.KAFKA_DEFAULTS_TOPIC,
                                         partition=self.scheduler.queue.partition))[1]
            self.close()

        elif isinstance(self.scheduler.queue.partition, list):
            self._consumers_offset = []
            for partition in self.scheduler.queue.partition:
                self._consumers_offset.append(self.scheduler.queue.consumer.get_watermark_offsets(
                    partition=TopicPartition(topic=defaults.KAFKA_DEFAULTS_TOPIC, partition=partition))[1])
            self.close()

    def close(self):
        if self.consumers_offset == self._consumers_offset:
            self.check_times += 1
            if self.check_times >= 2:
                self.slot.close()
                self.slot_.close()
                if reactor.running:
                    self.dbpool.close()
                    reactor.stop()
        else:
            self.consumers_offset = self._consumers_offset

    def open(self):
        self.set_up()
        self._start()
        if self.ip_ext:
            self.ip_fetch()
        reactor.run()

    def poll_msg(self, msg):
        self.scheduler.enqueue_request(msg)

    def ip_fetch(self, *args, **kwargs):
        pass

    @defer.inlineCallbacks
    def parse(self, result, *args, **kwargs):
        yield

    @defer.inlineCallbacks
    def process_item(self, item):
        try:
            yield self.dbpool.runInteraction(self.do_replace, item)
        # except pymysql.OperationalError:
        #     if self.report_connection_error:
        #         self.logger.error("Can't connect to MySQL")
        #         self.report_connection_error = False
        except:
            print(traceback.format_exc())

        # Return the item for the next stage
        defer.returnValue(item)

    @staticmethod
    def do_replace(tx, item):
        """Does the actual REPLACE INTO"""
        sql = ""
        args = (
        )
        tx.execute(sql, args)

    def ip_ext_get(self):
        self.ip = random.choice(self.ip_pool)

    def completed(self, timeoutCall, passthrough):
        if timeoutCall.active():
            timeoutCall.cancel()
        return passthrough

    def _download(self, request):
        if self.ip_ext:
            self.ip_ext_get()
            endpoint = TCP4ClientEndpoint(reactor, self.ip.split(':')[0], int(self.ip.split(':')[1]))
            agent = RedirectAgent(ProxyAgent(endpoint))
            response = agent.request(b"GET", request.encode('utf-8'), Headers(self.headers or {'User-Agent': []}))
            timeoutCall = reactor.callLater(self.settings.get('DOWNLOAD_TIMEOUT', 5), response.cancel)

            response.addBoth(self.completed, timeoutCall)
            return response
        else:
            agent = RedirectAgent(Agent(reactor))
            response = agent.request(b'GET', request.encode('utf-8'),
                                     Headers(self.headers or {'User-Agent': []}), None)
            timeoutCall = reactor.callLater(self.settings.get('DOWNLOAD_TIMEOUT', 5), response.cancel)

            response.addBoth(self.completed, timeoutCall)
            return response

    def _start(self):
        nextcall = self.next_fetch
        self.slot = Slot(nextcall)
        self.loop = self.slot.heartbeat
        nextcall_ = self.check
        self.slot_ = Slot(nextcall_)
        self.loop_ = self.slot_.heartbeat
        self.loop_spider = self.loop.start(self.settings.get('CURRENT_REQUEST', 0.1))
        self.loop_check = self.loop_.start(self.settings.get('CHECK_INTERVAL', 1800))
        nextcall_retry = self.retry_fetch
        self.slot_retry = Slot(nextcall_retry)
        self.loop_retry = self.slot_retry.heartbeat
        self.loop_retry_ = self.loop_retry.start(0.1)

    def next_fetch(self):
        url = self.scheduler.next_request()
        if not url:
            return
        self._download(url).addCallback(self.succeed_access, url).addErrback(self._retry, url)

    def _retry(self, e, url):
        self.crawl_queue.add(url)
        try:
            self.ip_pool.remove(self.ip)
        except Exception:
            self.logger.info('{} already remove'.format(self.ip))
        if len(self.ip_pool) == 0:
            self.ip_fetch()
        self.logger.info('something wrong with {}'.format(e))

    @defer.inlineCallbacks
    def succeed_access(self, response, url):
        if url in self.crawl_queue:
            self.crawl_queue.remove(url)
        d = response
        if d.code in self.ban_request:
            yield d
        elif d.code == 200:
            d = readBody(d)
            d.addCallback(self.parse)
            yield d

    def retry_fetch(self):
        if len(self.crawl_queue) > 0:
            url = self.crawl_queue.pop()
            self._download(url).addCallback(self.succeed_access, url).addErrback(self._retry, url)
        else:
            pass
