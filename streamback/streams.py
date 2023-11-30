import json
from logging import INFO, ERROR, WARNING, DEBUG
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

    def get_pending_messages_count(self):
        return 0


class KafkaStream(Stream):
    def __init__(self, kafka_host):
        self.kafka_hosts = listify(kafka_host)

    def initialize(self, group_name, flush_timeout=None, auto_flush_messages_count=None):
        self.group_name = group_name
        self.kafka_producer = self.create_kafka_producer()
        self.kafka_consumer = self.create_kafka_consumer()
        self.flush_timeout = flush_timeout
        self.auto_flush_messages_count = auto_flush_messages_count

    def create_kafka_producer(self):
        return Producer(
            {
                "bootstrap.servers": ",".join(self.kafka_hosts),
                "batch.size": 32768,
                "linger.ms": 1,
                "acks": 1,
                "queue.buffering.max.messages": 100000
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
        self.kafka_producer.poll(0)

        queue_size = self.get_pending_messages_count()

        log(DEBUG, "KAFKA_BUFFER_SIZE[SIZE={remaining_messages}]".format(
            remaining_messages=queue_size))

        if flush:
            self.flush()
        elif self.auto_flush_messages_count and queue_size >= self.auto_flush_messages_count:
            self.flush()

    def get_pending_messages_count(self):
        return len(self.kafka_producer)

    def flush(self):
        remaining_messages = self.kafka_producer.flush(timeout=self.flush_timeout or 5)
        if remaining_messages:
            log(ERROR, "FAILED_TO_FLUSH_MESSAGES[REMAINING={remaining_messages}]".format(
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
            msg = consumer.poll(0.1)

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

    def initialize(self, group_name, flush_timeout=5, auto_flush_messages_count=None):
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

    def get_pending_messages_count(self):
        return 0

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
        self.topics_prefix = None

        streams = streams_string.split("&")
        for stream in streams:
            setting_name = stream.split("=")[0]
            setting_value = stream.split("=")[1]

            if setting_name in ["main", "feedback"]:
                protocol = setting_value.split("://")[0]
                value = setting_value.split("://")[1]
                if protocol == "kafka":
                    stream = KafkaStream(value)
                elif protocol == "redis":
                    stream = RedisStream(value)

                if setting_name == "main":
                    self.main_stream = stream
                elif setting_name == "feedback":
                    self.feedback_stream = stream
            elif setting_name == "topics_prefix":
                self.topics_prefix = setting_value
            else:
                log(WARNING, "Unknown setting: %s" % setting_name)

        if not self.main_stream:
            raise InvalidSteamsString(streams_string, "Main stream is required")
