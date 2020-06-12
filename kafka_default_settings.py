SCHEDULER_DUPEFILTER_KEY = '{}s:dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'kafka_normal_process.dupefilter.RFPDupeFilter'
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': 'utf-8',
}

DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'

SCHEDULER_QUEUE_CLASS = 'kafka_normal_process.queues.KafkaQueue'
KAFKA_DEFAULTS_HOST = 'localhost:9092'
KAFKA_DEFAULTS_TOPIC = 'dy-tts'
KAFKA_DEFAULTS_CONSUMER_GROUP = 'gmdd66622'
BLOOMFILTER_BLOCK = 1
BLOOMFILTER_SIZE = 31
BLOOMFILTER_SEED = 6
POOL_SIZE = 15
