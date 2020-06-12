import redis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import kafka_default_settings as defaults

SETTINGS_PARAMS_MAP = {
    'REDIS_URL': 'url',
    'REDIS_HOST': 'host',
    'REDIS_PORT': 'port',
    'REDIS_ENCODING': 'encoding',
}


def create_start_request_consumer(name, bootstrap_servers):
    return Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': name,
        'auto.offset.reset': 'earliest'
    })


def create_consumer(name, bootstrap_servers):
    return Consumer({
        'bootstrap.servers': bootstrap_servers,
        # 'client.id': 'c1',
        'group.id': name,
        'auto.offset.reset': 'earliest'
    })


def create_producer(bootstrap_servers):
    return Producer({'bootstrap.servers': bootstrap_servers})


def get_redis(settings):
    params = defaults.REDIS_PARAMS.copy()
    params.update(settings['REDIS_PARAMS'])
    for source, dest in SETTINGS_PARAMS_MAP.items():
        val = settings.get(source)
        if val:
            params[dest] = val

    return redis.StrictRedis(**params)


def create_topic_client(topic, bootstrap_servers, partitions,
                        replication_factor):
    a = AdminClient({'bootstrap.servers': bootstrap_servers})

    new_topics = [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
    fs = a.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
