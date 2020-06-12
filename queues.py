from confluent_kafka import Consumer, Producer


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, producer: Producer, consumer: Consumer,
                 topic=None, serializer=None):
        self.producer = producer
        self.consumer = consumer
        self.topic = topic
        self.consumer.subscribe(topic)
        self.serializer = serializer
        self.partition = None

    def _encode_request(self, request):
        obj = request.encode('utf-8')
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return obj.decode('utf-8')

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def close(self):
        """Clear queue/stack"""
        self.consumer.close()


class KafkaQueue(Base):

    def __len__(self):
        """Return the length of the queue"""
        return 0

    def push(self, request):
        """Push a request"""
        self.producer.poll(0)
        self.producer.produce(self.topic[0], self._encode_request(request))
        self.producer.flush()

    def pop(self, timeout=0):
        """Pop a request"""

        request = self.consumer.poll(0)
        if len(self.consumer.assignment()) > 0:
            if len(self.consumer.assignment()) == 0:
                self.partition = self.consumer.assignment()[0].partition
            else:
                self.partition = []
                for partitions in self.consumer.assignment():
                    self.partition.append(partitions[0].partition)
        if request:
            data = self._decode_request(request.value())

            return data
