import picklecompat
from . import connection
from . import kafka_default_settings as defaults
from .utils import load_object


class Scheduler(object):
    def __init__(self, server, settings,
                 dupefilter_key=defaults.SCHEDULER_DUPEFILTER_KEY,
                 dupefilter_cls=defaults.SCHEDULER_DUPEFILTER_CLASS,
                 queue_cls=defaults.SCHEDULER_QUEUE_CLASS,
                 topic=defaults.KAFKA_DEFAULTS_TOPIC,
                 idle_before_close=0,
                 serializer=None
                 ):
        self.server = server
        self.consumer = None
        self.producer = None
        self.topic = topic
        self.dupefilter_cls = dupefilter_cls
        self.queue_cls = queue_cls
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_key = dupefilter_key
        if serializer is None:
            # TODO: deprecate pickle.
            self.serializer = picklecompat
        self.queue = None
        self.idle_before_close = idle_before_close
        self.settings = settings

    def open(self):
        self.consumer = connection.create_consumer(name=self.settings.get('KAFKA_DEFAULTS_CONSUMER_GROUP',
                                                                          defaults.KAFKA_DEFAULTS_CONSUMER_GROUP),
                                                   bootstrap_servers=self.settings.get('KAFKA_DEFAULTS_HOST',
                                                   defaults.KAFKA_DEFAULTS_HOST))
        self.producer = connection.create_producer(bootstrap_servers=self.settings.get('KAFKA_DEFAULTS_HOST',
                                                   defaults.KAFKA_DEFAULTS_HOST))
        try:
            self.df = load_object(self.dupefilter_cls)(
                server=self.server,
                key=self.dupefilter_key.format(self.settings['NAME']),
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate dupefilter clasPs '%s': %s",
                             self.dupefilter_cls, e)
        try:
            self.queue = load_object(self.queue_cls)(
                producer=self.producer,
                consumer=self.consumer,
                topic=[self.topic],
                serializer=self.serializer
            )

        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s",
                             self.queue_cls, e)

    def enqueue_request(self, request):
        if self.df.request_seen(request):
            return False
        self.queue.push(request)
        return True

    def next_request(self):
        request = self.queue.pop()
        if not request:
            return
        if self.df._request_seen(request):
            return None
        return request

    def close(self):
        self.df.clear()
        self.consumer.close()
