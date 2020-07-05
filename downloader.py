import re
import urllib

from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web.client import ProxyAgent, RedirectAgent, Agent
from twisted.web.http_headers import Headers

from . import kafka_default_settings as default


class Downloader(object):

    def __init__(self, settings=None):
        self.settings = settings
        self.headers = settings.get('HEADERS', default.HEADERS)
        pass

    def proxy_crawl(self, url, ip, reactor):
        endpoint = TCP4ClientEndpoint(reactor, ip.split(':')[0], int(ip.split(':')[1]),
                                      timeout=self.settings.get('DOWNLOAD_TIMEOUT', 10))
        agent = ProxyAgent(endpoint)
        response = agent.request(b"GET", url.encode('ascii'),
                                 Headers(self.headers))
        return response

    def normal_crawl(self, request, reactor):
        url = self.parse_uri(request)
        agent = RedirectAgent(Agent(reactor, connectTimeout=10))
        response = agent.request(b'GET', url.encode('ascii'),
                                 Headers(self.headers),
                                 None)
        return response

    def parse_uri(self, url):
        shem = re.split('://', url)[0]
        url = re.split('://', url)[1]
        request = shem + '://' + urllib.request.quote(url, encoding='utf-8')
        return request
