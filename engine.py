import logging
import random
import re
import time
import traceback
import urllib.request

import chardet
from confluent_kafka.cimpl import TopicPartition
from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.internet import task
from twisted.internet import threads
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web.client import Agent, readBody, ProxyAgent, RedirectAgent
from twisted.web.http_headers import Headers

import kspifka.kafka_default_settings as defaults
from kspifka.connection import get_redis
from kspifka.utils import SettingsWrapper
from . import kafka_default_settings as default
from .scheduler import Scheduler

logging.basicConfig(level=logging.NOTSET)


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
                 ip_ext=False, headers=None, ext=False, db_type=None, is_pic=False, *args, **kwargs):
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
        self.crawl_queue = {}
        self.logger = logging.getLogger('kafka-spider')
        self.db_type = db_type
        self.retry_times = self.settings.get('RETRY_TIMES', 7)
        self.queue = set([])
        self.is_pic = is_pic

    def set_up(self):
        if self.db_type:
            self.db_install()
        self.server = get_redis(self.settings)
        self.scheduler = self._scheduler(self.server, self.settings)
        self.scheduler.open()

    def db_install(self):
        conn_kwargs = self.settings.get('conn_kwargs', default.conn_kwargs)
        self.dbpool = adbapi.ConnectionPool(self.db_type,
                                            charset=self.settings.get('CHARSET', 'utf8'),
                                            use_unicode=self.settings.get('USE_UNICODE', True),
                                            connect_timeout=self.settings.get('CONNECT_TIMEOUT', 5),
                                            **conn_kwargs)

    @defer.inlineCallbacks
    def multithreading_fun(self, fun, fun_, *args):
        out = defer.Deferred()
        threads.deferToThread(fun, *args).addCallback(fun_)
        yield out

    def check(self):
        if isinstance(self.scheduler.queue.partition_s, int):
            self._consume_offset = self.scheduler.queue.consumer.get_watermark_offsets(
                partition=TopicPartition(topic=defaults.KAFKA_DEFAULTS_TOPIC,
                                         partition=self.scheduler.queue.partition_s))[1]
            self.close()

        elif isinstance(self.scheduler.queue.partition_s, list):
            self._consumers_offset = []
            for partition in self.scheduler.queue.partition_s:
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
    def parse(self, *args, **kwargs):
        yield

    @defer.inlineCallbacks
    def process_item(self, item):
        try:
            yield self.dbpool.runInteraction(self.do_replace, item)
        except Exception:
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
        ip = random.choice(self.ip_pool)
        return ip

    def download(self, request, ip):
        if ip:
            response = self.proxy_crawl(request, ip)
            return response
        else:
            response = self.normal_crawl(request)
            return response

    def _start(self):
        nextcall = self.next_fetch
        self.slot = Slot(nextcall)
        self.loop = self.slot.heartbeat
        self.loop.start(self.settings.get('CURRENT_REQUEST', 0.2))
        nextcall_ = self.check
        self.slot_ = Slot(nextcall_)
        self.loop_ = self.slot_.heartbeat
        self.loop_.start(self.settings.get('CHECK_INTERVAL', 1800))
        nextcall_retry = self.r_fetch
        self.slot_retry = Slot(nextcall_retry)
        self.loop_retry = self.slot_retry.heartbeat
        self.loop_retry.start(self.settings.get('RETRY_REQUEST', 0.2))

    @defer.inlineCallbacks
    def next_fetch(self, ip=None):

        url = self.scheduler.next_request()

        if not url:
            self.logger.info('get nothing from kafka')
            return
        if self.ip_ext:
            ip = self.ip_ext_get()
        url = url.replace('medium_jpg', 'large_jpg')
        self.logger.info('received {} from kafka'.format(url))
        self.download(url, ip).addCallback(self.succeed_access, url).addErrback(self._retry, url, ip)
        yield

    def _retry(self, e, url, ip):
        self.logger.info('something wrong with {}'.format(e), url)
        self.r_queue(url, ip)

    def r_queue(self, url, ip):
        self.logger.info(url)
        if isinstance(url, tuple):
            self.crawl_queue[url[0]] = url[1] + 1
        else:
            self.crawl_queue[url] = 1
        if self.ip_ext:
            try:
                self.ip_pool.remove(ip)
            except Exception:
                self.logger.info('{} already remove'.format(ip))
            if len(self.ip_pool) == 0:
                self.ip_fetch()

    @defer.inlineCallbacks
    def succeed_access(self, response, url):
        d = response
        if d.code in self.ban_request:
            return
        elif d.code == 200:
            d = readBody(d)
            d.addCallback(self.parse, url)
            d = ''
            yield d

    @defer.inlineCallbacks
    def analyse(self, response, url):
        if self.is_pic:
            yield self.parse(response, url)
        charset = chardet.detect(response)["encoding"]
        response = response.decode(charset)
        yield self.parse(response, url)

    def r_fetch(self, ip=None):
        if len(self.crawl_queue) > 0:
            url = self.crawl_queue.popitem()
            if url[1] > self.retry_times:
                self.scheduler.queue.push(url[0])
                return
            else:

                if self.ip_ext:
                    ip = self.ip_ext_get()
                self.r_download(url[0], ip).addCallback(self.succeed_access, url[0]).addErrback(self._retry, url, ip)
        else:
            return

    def r_download(self, request, ip):
        if ip:
            url = request
            response = self.proxy_crawl(url, ip)
            return response
        else:
            url = request
            response = self.normal_crawl(url)
            return response

    def proxy_crawl(self, url, ip):
        endpoint = TCP4ClientEndpoint(reactor, ip.split(':')[0], int(ip.split(':')[1]),
                                      timeout=self.settings.get('DOWNLOAD_TIMEOUT', 10))
        agent = ProxyAgent(endpoint)
        response = agent.request(b"GET", re.findall('(.+?)\(', url)[0].encode('ascii'),
                                 Headers(self.headers or {'User-Agent': [random.choice(default.USER_AGENT_LIST)]}))
        return response

    def normal_crawl(self, request):
        url = self.parse_uri(request)
        agent = RedirectAgent(Agent(reactor, connectTimeout=10))
        response = agent.request(b'GET', url.encode('utf-8'),
                                 Headers(self.headers or {'User-Agent': [random.choice(default.USER_AGENT_LIST)]}),
                                 None)
        return response

    def parse_uri(self, url):
        shem = re.split('://', url)[0]
        url = re.split('://', url)[1]
        request = shem + '://' + urllib.request.quote(url, encoding='utf-8')
        return request
