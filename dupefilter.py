import logging

import mmh3

from . import kafka_default_settings as defaults

logger = logging.getLogger(__name__)


class RFPDupeFilter:
    logger = logger

    def __init__(self, server, key, debug=False, **kwargs):
        self.server = server
        self.key = key
        self.debug = debug
        self.seeds = kwargs.get("seeds", defaults.BLOOMFILTER_SEED)
        self.blockNum = kwargs.get("blockNum", defaults.BLOOMFILTER_BLOCK)
        self.bit_size = 1 << kwargs.get("bit_size", defaults.BLOOMFILTER_SIZE)
        self.logdupes = True

    def request_seen(self, request):

        if self.isContains(request):
            return True
        self.insert(request)
        return False

    def isContains(self, str_input):
        if not str_input:
            return False

        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            # 判断是否在集合中，要求所有的哈希函数得到的偏移值都是1
            # 如果getbit为0，则说明此元素不在集合中，跳出判断
            if self.server.getbit(name, loc % self.bit_size) == 0:
                break
        else:
            # for中所有条件均未跳出，说明所有的偏移值都是1，元素在集合中
            return True
        return False

    def insert(self, str_input):
        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            self.server.setbit(name, loc % self.bit_size, 1)

    def close(self, reason=''):

        self.clear()

    def clear(self):
        """Clears fingerprints data."""
        self.server.delete(self.key)

    def log(self, request, spider):

        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

    def _request_seen(self, request):

        fp = request
        if self._isContains(fp):
            return True
        self.insert(fp)
        return False

    def _isContains(self, str_input):
        if not str_input:
            return False

        name = self.key + '_crawled' + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            # 判断是否在集合中，要求所有的哈希函数得到的偏移值都是1
            # 如果getbit为0，则说明此元素不在集合中，跳出判断
            if self.server.getbit(name, loc % self.bit_size) == 0:
                break
        else:
            # for中所有条件均未跳出，说明所有的偏移值都是1，元素在集合中
            return True
        return False
