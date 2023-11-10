import json
from logging import INFO, ERROR
import redis
import time
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition
from .exceptions import FeedbackTimeout, InvalidSteamsString
from .message import Message, KafkaMessage
from .utils import log, listify


class Stream(object):
    def initialize(self, group_name, timeout=10):
        raise NotImplementedError

    def send(self, topic, payload, key=None, flush=False):
        raise NotImplementedError

    def read_stream(self, streamback, topics, timeout=None):
        raise NotImplementedError

    def serialize_payload(self, data):
        return json.dumps(data).encode("utf-8")

    def deserialize_payload(self, data):
        return json.loads(data.decode("utf-8"))

    def flush(self):
        raise NotImplementedError


class KafkaStream(Stream):
    def __init__(self, kafka_host):
        self.kafka_hosts = listify(kafka_host)

    def initialize(self, group_name, flush_timeout=10):
        self.group_name = group_name
        self.kafka_producer = self.create_kafka_producer()
        self.kafka_consumer = self.create_kafka_consumer()
        self.flush_timeout = flush_timeout

    def create_kafka_producer(self):
        return Producer(
            {
                "bootstrap.servers": ",".join(self.kafka_hosts),
                "linger.ms": 500,
                "batch.size": 8192,
                "acks": 1,
            }
        )

    def create_kafka_consumer(self):
        return Consumer(
            {
                "bootstrap.servers": ",".join(self.kafka_hosts),
                "group.id": self.group_name,
                "auto.offset.reset": "earliest",
                "fetch.min.bytes": 1,
                "enable.auto.commit": False
            }
        )

    def send(self, topic, payload, key=None, flush=False):
        self.kafka_producer.produce(topic, self.serialize_payload(payload), key=key)
        if flush:
            self.flush()

    def flush(self):
        self.kafka_producer.poll(0)
        remaining_messages = self.kafka_producer.flush(timeout=self.flush_timeout)
        if remaining_messages:
            log(INFO, "FAILED_TO_FLUSH_MESSAGES[REMAINING={remaining_messages}]".format(
                remaining_messages=remaining_messages))
            return False
        else:
            log(INFO, "SUCCESSFULLY_FLUSHED_MESSAGES")

        return True

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
            msg = consumer.poll(1)

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

                message = KafkaMessage(
                    kafka_message=msg,
                    streamback=streamback,
                    topic=msg.topic(),
                    payload=payload,
                    key=msg.key(),
                )

                message.ack = lambda: consumer.commit(msg)

                yield message

        consumer.close()


class RedisStream(Stream):
    def __init__(self, redis_host):
        self.redis_host = redis_host

    def initialize(self, group_name, flush_timeout=10):
        self.group_name = group_name
        self.flush_timeout = flush_timeout
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

            message = Message(
                streamback=streamback,
                topic=value[0].decode(),
                payload=payload,
                key=None,
            )

            message.ack = lambda: True

            yield message

    def flush(self):
        pass


class ParsedStreams(object):
    def __init__(self, streams_string):
        self.main_stream = None
        self.feedback_stream = None

        streams = streams_string.split("&")
        for stream in streams:
            stream_category = stream.split("=")[0]
            stream_value = stream.split("=")[1]
            stream_type = stream_value.split("://")[0]
            stream_hosts = stream_value.split("://")[1]
            if stream_type == "kafka":
                stream = KafkaStream(stream_hosts)
            elif stream_type == "redis":
                stream = RedisStream(stream_hosts)
            else:
                raise Exception("Unknown stream type: %s" % stream_type)

            if stream_category == "main":
                self.main_stream = stream
            elif stream_category == "feedback":
                self.feedback_stream = stream

        if not self.main_stream:
            raise InvalidSteamsString(streams_string, "Main stream is required")
