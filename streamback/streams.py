import json
from logging import INFO, ERROR
import redis
import time
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from .exceptions import FeedbackTimeout
from .message import Message
from .utils import log, listify


class Stream(object):
    def initialize(self, group_name):
        raise NotImplementedError

    def send(self, topic, payload, key=None, flush=False):
        raise NotImplementedError

    def read_stream(self, streamback, topics, timeout=None):
        raise NotImplementedError

    def serialize_payload(self, data):
        return json.dumps(data).encode("utf-8")

    def deserialize_payload(self, data):
        return json.loads(data.decode("utf-8"))


class KafkaStream(Stream):
    def __init__(self, kafka_host):
        self.kafka_hosts = listify(kafka_host)

    def initialize(self, group_name):
        self.group_name = group_name
        self.kafka_producer = self.create_kafka_producer()
        self.kafka_consumer = self.create_kafka_consumer()

    def create_kafka_producer(self):
        return Producer(
            {
                "bootstrap.servers": ",".join(self.kafka_hosts),
                "linger.ms": 0,
                "batch.size": 1,
                "acks": 1,
            }
        )

    def create_kafka_consumer(self):
        return Consumer(
            {
                "bootstrap.servers": ",".join(self.kafka_hosts),
                "group.id": self.group_name,
                "auto.offset.reset": "latest",
                "auto.commit.interval.ms": 1,
                "fetch.min.bytes": 1,
                "fetch.wait.max.ms": 0,
                "enable.auto.commit": True,
            }
        )

    def send(self, topic, payload, key=None, flush=False):
        self.kafka_producer.produce(topic, self.serialize_payload(payload), key=key)
        if flush:
            self.kafka_producer.flush()

    def read_stream(self, streamback, topics, timeout=None):
        consumer = self.create_kafka_consumer()
        consumer.subscribe(topics)
        begin = time.time()

        log(
            INFO,
            "MAIN_STREAM_LISTENING[topics={topics}]".format(
                topics=topics
            ),
        )

        while True:
            msg = consumer.poll(0.5)

            if timeout:
                time_since_begin = time.time() - begin
                if time_since_begin > timeout:
                    return

            if not msg:
                continue

            if msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log(ERROR, "Topic {} does not exist".format(msg.topic()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                payload = self.deserialize_payload(msg.value())

                message = Message(
                    streamback=streamback,
                    topic=msg.topic(),
                    payload=payload,
                    key=msg.key(),
                )

                consumer.commit(msg)

                yield message

        consumer.close()


class RedisStream(Stream):
    def __init__(self, redis_host):
        self.redis_host = redis_host

    def initialize(self, group_name):
        self.group_name = group_name
        self.redis_client = redis.Redis(
            host=self.redis_host.split(":")[0],
            port=int(self.redis_host.split(":")[1]),
            password=None,
        )

    def send(self, topic, payload, key=None, flush=None):
        self.redis_client.lpush(topic, self.serialize_payload(payload))
        self.redis_client.expire(topic, 300)

    def read_stream(self, streamback, topics, timeout=None):
        while True:
            value = self.redis_client.brpop(topics, timeout=timeout)

            if value is None:
                raise FeedbackTimeout(topics=topics, timeout=timeout)

            payload = self.deserialize_payload(value[1])

            yield Message(
                streamback=streamback,
                topic=value[0].decode(),
                payload=payload,
                key=None,
            )
