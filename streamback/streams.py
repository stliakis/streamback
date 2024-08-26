import json
from logging import INFO, ERROR, WARNING, DEBUG
import redis
import time
from confluent_kafka import (
    Producer,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from .exceptions import FeedbackTimeout, InvalidSteamsString
from .message import Message, KafkaMessage
from .utils import log, listify


class Stream(object):
    initialized = False

    def initialize(self, group_name, timeout=10):
        raise NotImplementedError

    def send(self, topic, payload, key=None, flush=False):
        raise NotImplementedError

    def read_stream(self, streamback, topics, timeout=None, on_tick=None):
        raise NotImplementedError

    def serialize_payload(self, data):
        return json.dumps(data).encode("utf-8")

    def deserialize_payload(self, data):
        return json.loads(data.decode("utf-8"))

    def flush(self):
        raise NotImplementedError

    def get_pending_messages_count(self):
        return 0

    def is_initialized(self):
        return self.initialized

    def close(self):
        raise NotImplementedError

    def get_message_count_in_topics(self, topic):
        raise NotImplementedError


class KafkaStream(Stream):
    def __init__(self, group_name, kafka_host, authentication=None):
        self.kafka_hosts = listify(kafka_host)
        self.authentication = authentication
        self.group_name = group_name
        self.kafka_consumer = None
        self.kafka_producer = None
        self.service_kafka_consumer = None
        self.closed = False

    def initialize(
            self,
            flush_timeout=None,
            auto_flush_messages_count=None,
    ):
        self.kafka_producer = self.create_kafka_producer()
        self.kafka_consumer = self.create_kafka_consumer()
        self.flush_timeout = flush_timeout
        self.auto_flush_messages_count = auto_flush_messages_count
        self.initialized = True

    def close(self):
        self.closed = True
        if self.kafka_producer:
            self.kafka_producer.flush()
        if self.kafka_consumer:
            self.kafka_consumer.close()

    def extend_config_with_authentication(self, config):
        if isinstance(self.authentication, UsernamePasswordAuthentication):
            config["sasl.mechanism"] = "PLAIN"
            config["security.protocol"] = "SASL_PLAINTEXT"
            config["sasl.username"] = self.authentication.username
            config["sasl.password"] = self.authentication.password

        return config

    def create_kafka_producer(self):
        config = {
            "bootstrap.servers": ",".join(self.kafka_hosts),
            "batch.size": 32768,
            "linger.ms": 1,
            "acks": 1,
            "queue.buffering.max.messages": 100000
        }

        config = self.extend_config_with_authentication(config)

        log(DEBUG, "KAFKA_PRODUCER_CONFIG[CONFIG={config}]".format(config=config))

        return Producer(config)

    def create_kafka_consumer(self, group_name=None):
        config = {
            "bootstrap.servers": ",".join(self.kafka_hosts),
            "group.id": group_name or self.group_name,
            "max.partition.fetch.bytes": 128,
            "auto.offset.reset": "earliest",
            "fetch.min.bytes": 1,
            "enable.auto.commit": False,
        }

        config = self.extend_config_with_authentication(config)

        log(DEBUG, "KAFKA_CONSUMER_CONFIG[CONFIG={config}]".format(config=config))

        return Consumer(config)

    def send(self, topic, payload, key=None, flush=False):
        self.kafka_producer.produce(topic, self.serialize_payload(payload), key=key)
        self.kafka_producer.poll(0)

        queue_size = self.get_pending_messages_count()

        log(
            DEBUG,
            "KAFKA_BUFFER_SIZE[SIZE={remaining_messages}]".format(
                remaining_messages=queue_size
            ),
        )

        if flush:
            self.flush()
        elif (
                self.auto_flush_messages_count
                and queue_size >= self.auto_flush_messages_count
        ):
            self.flush()

    def get_pending_messages_count(self):
        return len(self.kafka_producer)

    def flush(self):
        remaining_messages = self.kafka_producer.flush(timeout=self.flush_timeout or 5)
        if remaining_messages:
            log(
                ERROR,
                "FAILED_TO_FLUSH_MESSAGES[REMAINING={remaining_messages}]".format(
                    remaining_messages=remaining_messages
                ),
            )
            return False
        else:
            log(INFO, "SUCCESSFULLY_FLUSHED_MESSAGES")

        return True

    def get_or_create_service_consumer(self):
        if not self.service_kafka_consumer:
            self.service_kafka_consumer = self.create_kafka_consumer(self.group_name)

        return self.service_kafka_consumer

    def get_message_count_in_topics(self, topics):
        consumer = self.get_or_create_service_consumer()

        total_unread_messages = 0

        for topic in topics:
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions

            if not partitions:
                continue
                # log(WARNING, "NO_PARTITIONS_FOR_TOPIC[topic={topic}]".format(topic=topic))

            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                low, high = consumer.get_watermark_offsets(tp)
                committed_offsets = consumer.committed([tp])
                current_offset = committed_offsets[0].offset
                unread_messages = high - current_offset
                total_unread_messages += unread_messages

        return total_unread_messages

    def read_stream(self, streamback, topics, timeout=None, on_tick=None):
        consumer = self.kafka_consumer

        begin = time.time()

        log(
            INFO,
            "MAIN_STREAM_LISTENING[topics={topics}]".format(topics=topics),
        )

        consumer.subscribe(topics)

        while self.closed is False:
            if on_tick:
                on_tick()

            messages = consumer.consume(100, 0.05)

            for msg in messages:
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


class RedisStream(Stream):
    def __init__(self, group_name, redis_host):
        self.redis_host = redis_host
        self.group_name = group_name
        self.redis_client = None

    def initialize(
            self,
            flush_timeout=5,
            auto_flush_messages_count=None,
            authentication=None
    ):
        self.flush_timeout = flush_timeout
        self.authentication = authentication
        self.redis_client = redis.Redis(
            host=self.redis_host.split(":")[0],
            port=int(self.redis_host.split(":")[1]),
            password=None,
        )
        self.initialized = True

    def send(self, topic, payload, key=None, flush=None):
        self.redis_client.lpush(topic, self.serialize_payload(payload))
        self.redis_client.expire(topic, 300)

    def get_pending_messages_count(self):
        return 0

    def read_stream(self, streamback, topics, timeout=None, on_tick=None):
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

    def close(self):
        if self.redis_client:
            self.redis_client.close()


class UsernamePasswordAuthentication(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password


class ParsedStreams(object):
    def __init__(self, group_name, streams_string):
        self.group_name = group_name
        self.main_stream = None
        self.feedback_stream = None
        self.topics_prefix = None
        self.authentication = None

        streams = streams_string.split("&")
        for stream in streams:
            setting_name = stream.split("=")[0]
            setting_value = stream.split("=")[1]

            if setting_name == "topics_prefix":
                self.topics_prefix = setting_value
            elif setting_name == "group":
                self.group_name = setting_value

        for stream in streams:
            setting_name = stream.split("=")[0]
            setting_value = stream.split("=")[1]

            if setting_name in ["main", "feedback"]:
                protocol = setting_value.split("://")[0]
                value = setting_value.split("://")[1]
                if protocol == "kafka":
                    if "@" in value:
                        auth_value = value.split(":")[0]
                        authentication = UsernamePasswordAuthentication(
                            username=auth_value.split("@")[0],
                            password=auth_value.split("@")[1],
                        )
                        value = ":".join(value.split(":")[1:])
                    else:
                        authentication = None

                    stream = KafkaStream(self.group_name, value, authentication=authentication)
                elif protocol == "redis":
                    stream = RedisStream(self.group_name, value)

                if setting_name == "main":
                    self.main_stream = stream
                elif setting_name == "feedback":
                    self.feedback_stream = stream
            elif setting_name in ["topics_prefix", "group"]:
                continue
            else:
                log(WARNING, "Unknown setting: %s" % setting_name)

        if not self.main_stream:
            raise InvalidSteamsString(streams_string, "Main stream is required")
