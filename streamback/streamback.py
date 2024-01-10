import inspect
import logging
import time
import traceback
from logging import INFO, ERROR, WARNING, DEBUG
import uuid

from .exceptions import CouldNotFlushToMainStream, FeedbackError, InvalidMessageType
from .context import ConsumerContext
from .events import Events
from .listener import Listener
from .streams import KafkaStream, ParsedStreams
from .utils import log


class Streamback(object):
    def __init__(
        self,
        name,
        streams=None,
        topics_prefix=None,
        main_stream=None,
        feedback_stream=None,
        feedback_timeout=60,
        feedback_ttl=300,
        main_stream_timeout=5,
        auto_flush_messages_count=200,
        on_exception=None,
        callbacks=None,
        log_level="INFO",
        **kwargs
    ):
        self.initialize_logger(log_level)

        self.callbacks = callbacks or []
        self.name = name
        self.feedback_timeout = feedback_timeout
        self.feedback_ttl = feedback_ttl
        self.on_exception = on_exception
        self.topics_prefix = topics_prefix

        if streams:
            parsed_streams = ParsedStreams(streams)
            self.main_stream = parsed_streams.main_stream
            self.feedback_stream = parsed_streams.feedback_stream
            self.topics_prefix = parsed_streams.topics_prefix
        else:
            self.main_stream = main_stream
            self.feedback_stream = feedback_stream

        if self.main_stream:
            self.main_stream.initialize(
                name,
                flush_timeout=main_stream_timeout,
                auto_flush_messages_count=auto_flush_messages_count,
            )

        if self.feedback_stream:
            self.feedback_stream.initialize(name)

            if isinstance(self.feedback_stream, KafkaStream):
                log(
                    WARNING,
                    "kafka is not recommended for feedback stream, use redis instead",
                )

        self.listeners = {}
        self.routers = []

        log(
            INFO,
            "STREAMBACK_INITIALIZED[name={name},main_stream={main_stream},feedback_stream={feedback_stream}]".format(
                name=name,
                main_stream=self.main_stream,
                feedback_stream=self.feedback_stream,
            ),
        )

    def initialize_logger(self, log_level):
        logger = logging.getLogger("streamback")
        logger.setLevel(
            {
                "INFO": logging.INFO,
                "ERROR": logging.ERROR,
                "DEBUG": logging.DEBUG,
                "WARNING": logging.WARNING,
            }.get(log_level.upper(), logging.INFO)
        )

        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "streamback - %(levelname)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    def get_payload_metadata(self):
        return {"source_group": self.name}

    def flush(self):
        if self.feedback_stream:
            self.feedback_stream.flush()

        if self.main_stream:
            return self.main_stream.flush()

        return 0

    def get_topic_real_name(self, topic):
        if self.topics_prefix and not topic.startswith(self.topics_prefix):
            return "%s.%s" % (self.topics_prefix, topic)

        return topic

    def check_message_value_type(self, value):
        if value is None:
            return value

        if not isinstance(value, dict):
            raise InvalidMessageType(
                "The message must be a dict or None, got %s" % type(value)
            )

        return value

    def send(
        self,
        topic,
        value=None,
        payload=None,
        key=None,
        event=Events.MAIN_STREAM_MESSAGE,
        flush=False,
    ):
        topic = self.get_topic_real_name(topic)

        payload = payload or {}

        correlation_id = str(uuid.uuid4())
        feedback_topic = "streamback_feedback_%s" % correlation_id

        payload.update(
            {
                "value": payload.get("value") or value,
                "event": event,
                "feedback_topic": feedback_topic,
            }
        )

        payload["value"] = self.check_message_value_type(payload["value"])

        payload.update(self.get_payload_metadata())

        begin = time.time()

        self.main_stream.send(topic, payload, key=key, flush=flush)

        took = (time.time() - begin) * 1000

        log(
            INFO,
            "DEBUG[topic={topic}, key={key}, payload={payload}, took_ms={took}]".format(
                topic=topic, key=key, payload="{...}", took=round(took, 2)
            ),
        )

        return FeedbackLane(
            streamback=self,
            feedback_topic=feedback_topic,
            feedback_stream=self.feedback_stream,
        )

    def continue_stream(self, feedback_topic):
        return FeedbackLane(
            streamback=self,
            feedback_topic=feedback_topic,
            feedback_stream=self.feedback_stream,
        )

    def send_feedback(self, topic, value=None, payload=None):
        if not self.feedback_stream:
            log(
                ERROR,
                "CANNOT_SEND_FEEDBACK[reason=feedback stream missing]".format(
                    topic=topic
                ),
            )
            return

        payload = payload or {"value": value}

        payload["value"] = self.check_message_value_type(payload.get("value"))

        payload.update({"event": Events.FEEDBACK_MESSAGE, "source_group": self.name})

        log(
            DEBUG,
            "SENDING_FEEDBACK[event={event} topic={topic} payload={payload}]".format(
                topic=topic, event=Events.FEEDBACK_MESSAGE, payload=payload
            ),
        )

        self.feedback_stream.send(topic, payload)

    def send_feedback_end(self, topic, with_error=None):
        if with_error is not None:
            event = Events.FEEDBACK_END_WITH_ERROR
        else:
            event = Events.FEEDBACK_END

        payload = {"event": event, "error": with_error}

        payload.update(self.get_payload_metadata())

        log(
            DEBUG,
            "SENDING_FEEDBACK[event={event} topic={topic} payload={payload}]".format(
                topic=topic,
                event=event,
                payload=payload,
            ),
        )

        self.feedback_stream.send(topic, payload)

    def listen(self, topic=None, retry=None, input=None):
        def decorator(func):
            if inspect.isclass(func) and issubclass(func, Listener):
                listener_class = func
                listener = listener_class(
                    topic=topic, retry_strategy=retry, input=input
                )
                self.add_listener(listener)
            else:
                if not topic:
                    raise Exception(
                        "topic is required when using a function as a listener"
                    )
                self.add_listener(
                    Listener(
                        topic=topic, function=func, retry_strategy=retry, input=input
                    )
                )

            def wrapper_func(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper_func

        return decorator

    def include_router(self, router):
        self.routers.append(router)
        for listener in router.listeners:
            self.add_listener(listener)

    def start(self):
        topics = list(self.listeners.keys())
        for message in self.main_stream.read_stream(
            streamback=self, topics=topics, timeout=None
        ):
            log(
                DEBUG,
                "RECEIVED[message={message}]".format(message=message),
            )

            context = ConsumerContext(streamback=self)

            listeners = self.listeners.get(message.topic, [])
            failed_listeners = []
            for listener in listeners:
                try:
                    for callback in self.get_callbacks():
                        callback.on_consume_begin(self, listener, context, message)
                    listener.try_to_consume(context, message)
                    for callback in self.get_callbacks():
                        callback.on_consume_end(self, listener, context, message)
                except Exception as e:
                    log(
                        ERROR,
                        "LISTENER_ERROR[listener={listener} error={error}]".format(
                            listener=listener, error=str(e)
                        ),
                    )
                    traceback.print_exc()
                    failed_listeners.append([listener, e, context, message])
                    for callback in self.get_callbacks():
                        callback.on_consume_end(
                            self, listener, context, message, exception=e
                        )
                finally:

                    message.ack()

            if failed_listeners:
                errors = []
                for failed_listener in failed_listeners:
                    self.consume_exception(*failed_listener)
                    errors.append(repr(failed_listener[1]))

                if self.feedback_stream and message.feedback_topic:
                    self.send_feedback_end(
                        message.feedback_topic, with_error=", ".join(errors)
                    )
            elif not failed_listeners:
                if self.feedback_stream and message.feedback_topic:
                    self.send_feedback_end(message.feedback_topic)

    def get_callbacks(self):
        return self.callbacks

    def consume_exception(self, listener, exception, context, message):
        if self.on_exception:
            self.on_exception(listener, context, message, exception)
        for callback in self.get_callbacks():
            callback.on_consume_exception(self, listener, exception, context, message)

    def add_listener(self, listener):
        listener.topic = self.get_topic_real_name(listener.topic)
        self.listeners.setdefault(listener.topic, []).append(listener)
        return self

    def add_callback(self, callback):
        self.callbacks.append(callback)
        return self


class FeedbackLane(object):
    def __init__(self, streamback, feedback_topic, feedback_stream):
        self.streamback = streamback
        self.feedback_topic = feedback_topic
        self.feedback_stream = feedback_stream

    def stream(self, from_group, timeout=None):
        for feedback in self.stream_messages(from_group=from_group, timeout=timeout):
            if feedback:
                yield feedback.value
            else:
                return

    def stream_messages(self, from_group, timeout=None):
        self.assert_feedback_stream()

        log(
            INFO,
            "LISTENING_FOR_FEEDBACK[feedback_topic={feedback_topic},group={from_group}]".format(
                from_group=from_group, feedback_topic=self.feedback_topic
            ),
        )

        flushed = self.streamback.main_stream.flush()
        if not flushed:
            raise CouldNotFlushToMainStream(
                "Could not flush to main stream, feedback will not be received"
            )

        for feedback in self.feedback_stream.read_stream(
            streamback=self,
            topics=[self.feedback_topic],
            timeout=timeout or self.streamback.feedback_timeout,
        ):
            if from_group and feedback.source_group != from_group:
                continue

            log(DEBUG, "RECEIVED_FEEDBACK[{feedback}]".format(feedback=feedback))

            if feedback.event == Events.FEEDBACK_END:
                return
            elif feedback.event == Events.FEEDBACK_END_WITH_ERROR:
                raise FeedbackError(feedback.error)
            else:
                yield feedback

    def read(self, from_group, timeout=None, map=None):
        message = self.read_message(from_group, timeout=timeout)
        print("message:", message)
        if message:
            if map:
                return map(**message.value)
            return message.value

    def read_message(self, from_group, timeout=None):
        self.assert_feedback_stream()
        for feedback in self.stream_messages(from_group=from_group, timeout=timeout):
            if feedback.event == Events.FEEDBACK_MESSAGE:
                return feedback

    def flush(self):
        self.streamback.main_stream.flush()

    def assert_feedback_stream(self):
        if not self.feedback_stream:
            raise Exception(
                "Feedback stream not configured, Streamback is configured as a one way stream"
            )
