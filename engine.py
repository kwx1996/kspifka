import logging
import random
import time
import traceback

import chardet
from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.internet import task
from twisted.web.client import readBody

from kspifka.connection import get_redis
from kspifka.offset_monitor import check
from kspifka.utils import SettingsWrapper, next_callable
from . import kafka_default_settings as default
from .downloader import Downloader
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
                 ip_ext=False, ext=False, db_type=None, is_pic=False, order=None, *args, **kwargs):
        super(engine, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger()
        self.topic = topic
        self.slot = Slot()
        self._scheduler = scheduler
        self.settings = SettingsWrapper().load(default=settings)
        self.start_time = time.time()
        self.ip_ext = ip_ext
        self.ban_request = []
        self.ip_pool = []
        self.ip = None
        self.ext = ext
        self.crawl_queue = {}
        self.logger = logging.getLogger('kafka-spider')
        self.db_type = db_type
        self.retry_times = self.settings.get('RETRY_TIMES', 7)
        self.queue = set([])
        self.is_pic = is_pic
        self.order = order
        self.downloader = Downloader(settings=settings)

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

    # a order,like 'sh kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_id'
    def _check(self):
        res = check(self.order)
        if res == 'finished':
            self.close()

    def close(self):
        self.slot.close()
        if reactor.running:
            self.dbpool.close()
            reactor.stop()

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
        args = ()
        tx.execute(sql, args)

    def ip_ext_get(self):
        ip = random.choice(self.ip_pool)
        return ip

    def download(self, request, ip):
        if ip:
            url = request
            response = self.downloader.proxy_crawl(url, ip, reactor)
            return response
        else:
            url = request
            response = self.downloader.normal_crawl(url, reactor)
            return response

    def _start(self):
        self.loop = next_callable(self.next_fetch)
        self.loop.start(self.settings.get('CURRENT_REQUEST', 0.2))
        self.loop_ = next_callable(self._check)
        self.loop_.start(self.settings.get('CHECK_INTERVAL', 1800))
        self.loop_r = next_callable(self.r_fetch())
        self.loop_r.start(self.settings.get('RETRY_REQUEST', 0.2))

    @defer.inlineCallbacks
    def next_fetch(self, ip=None):
        url = self.scheduler.next_request()
        if not url:
            self.logger.info('get nothing from kafka')
            return
        if self.ip_ext:
            ip = self.ip_ext_get()
        self.logger.info('received {} from kafka'.format(url))
        self.download(url, ip).addCallback(self.succeed_access, url).addErrback(self._retry, url, ip)
        yield

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

    def _retry(self, e, url, ip):
        self.logger.info('something wrong with {}'.format(e), url)
        self.r_queue(url, ip)

    def r_queue(self, url, ip):
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
            response = self.downloader.proxy_crawl(url, ip, reactor)
            return response
        else:
            url = request
            response = self.downloader.normal_crawl(url, reactor)
            return response

    @defer.inlineCallbacks
    def analyse(self, response, url):
        if self.is_pic:
            yield self.parse(response, url)
        charset = chardet.detect(response)["encoding"]
        response = response.decode(charset)
        yield self.parse(response, url)
