import inspect
import logging
import time
import traceback
from logging import INFO, ERROR, WARNING, DEBUG
import uuid
import signal
import sys
import multiprocessing

from .feedback_lane import FeedbackLane
from .exceptions import InvalidMessageType
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
            retry_strategy=None,
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
        self.main_stream_timeout = main_stream_timeout
        self.default_retry_strategy = retry_strategy
        self.auto_flush_messages_count = auto_flush_messages_count

        if streams:
            parsed_streams = ParsedStreams(streams)
            self.main_stream = parsed_streams.main_stream
            self.feedback_stream = parsed_streams.feedback_stream
            self.topics_prefix = parsed_streams.topics_prefix
            if parsed_streams.group_name:
                self.name = parsed_streams.group_name
                log(
                    WARNING,
                    "STREAMBACK_GROUP_NAME_OVERRIDE[name={name}]".format(name=self.name),
                )
        else:
            self.main_stream = main_stream
            self.feedback_stream = feedback_stream

        self.listeners = {}
        self.routers = []

    def initialize_main_stream(self):
        if self.main_stream and not self.main_stream.is_initialized():
            self.main_stream.initialize(
                self.name,
                flush_timeout=self.main_stream_timeout,
                auto_flush_messages_count=self.auto_flush_messages_count
            )

    def initialize_feedback_stream(self):
        if self.feedback_stream and not self.feedback_stream.is_initialized():
            self.feedback_stream.initialize(self.name)

            if isinstance(self.feedback_stream, KafkaStream):
                log(
                    WARNING,
                    "kafka is not recommended for feedback stream, use redis instead",
                )

    def initialize_streams(self):
        self.initialize_main_stream()
        self.initialize_feedback_stream()

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

    def on_fork(self):
        for callback in self.get_callbacks():
            callback.on_fork()

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
        self.initialize_streams()

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
            "SENDING[topic={topic}, key={key}, payload={payload}, took_ms={took}]".format(
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

    def listen(self, topic=None, retry=None, input=None, concurrency=None):
        def decorator(func):
            if inspect.isclass(func) and issubclass(func, Listener):
                listener_class = func
                listener = listener_class(
                    topic=topic, retry_strategy=retry or self.default_retry_strategy, input=input,
                    concurrency=concurrency
                )
                self.add_listener(listener)
            else:
                if not topic:
                    raise Exception(
                        "topic is required when using a function as a listener"
                    )
                self.add_listener(
                    Listener(
                        topic=topic, function=func, retry_strategy=retry or self.default_retry_strategy, input=input,
                        concurrency=concurrency
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

    def start_listener(self, listener):
        self.listeners = {listener.topic: [listener]}
        self.initialize_streams()
        self.on_fork()

        for message in self.main_stream.read_stream(
                streamback=self, topics=[listener.topic], timeout=None
        ):
            log(
                INFO,
                "RECEIVED[message={message}]".format(message=message),
            )

            context = ConsumerContext(streamback=self)

            failure = None
            try:
                for callback in self.get_callbacks():
                    callback.on_consume_begin(self, listener, context, message)

                begin = time.time()
                listener.try_to_consume(context, message)
                end = time.time()

                log(DEBUG, "CONSUME_SUCCESS[listener={listener}, took_ms={took}]".format(listener=listener,
                                                                                         took=round(
                                                                                             (end - begin) * 1000, 2)))

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
                failure = [listener, e, context, message]
                for callback in self.get_callbacks():
                    callback.on_consume_end(
                        self, listener, context, message, exception=e
                    )
            finally:
                message.ack()

            if failure:
                try:
                    self.consume_exception(*failure)
                except Exception as ex:
                    log(
                        ERROR,
                        "EXCEPTION_WHILE_CONSUMING_EXCEPTION[listener={listener} error={error}]".format(
                            listener=listener, error=str(ex)
                        ),
                    )
                    traceback.print_exc()
                if self.feedback_stream and message.feedback_topic:
                    self.send_feedback_end(
                        message.feedback_topic, with_error=repr(failure[1])
                    )
            else:
                if self.feedback_stream and message.feedback_topic:
                    self.send_feedback_end(message.feedback_topic)

    def _start_listener(self, listener):
        def signal_handler(sig, frame):
            log(INFO, "LISTENER_PROCESS_KILLED[topic={topic}]".format(topic=listener.topic))
            self.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            self.start_listener(listener)
        except (KeyboardInterrupt, Exception):
            self.close()

    def start(self):
        listener_processes = []
        all_listeners = []

        for topic in self.listeners:
            for listener in self.listeners[topic]:
                all_listeners.append(listener)

        log(INFO,
            "STARTING_PROCESSES[num={num},listeners={listeners}]".format(num=len(all_listeners), listeners=",".join([
                "%s[procs=%s]" % (listener, listener.concurrency) for listener in
                all_listeners if
                listener.function
            ])))

        for listener in all_listeners:
            for i in range(int(listener.concurrency)):
                process = multiprocessing.Process(target=self._start_listener, args=(listener,))
                process.start()
                listener_processes.append((listener, process))

        def signal_handler(sig, frame):
            log(INFO, "MASTER_PROCESS_KILLED")
            for listener, process in listener_processes:
                process.terminate()
                process.join()
            self.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                for callback in self.get_callbacks():
                    callback.on_master_tick(listener_processes)
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

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

    def add_callback(self, *callback):
        self.callbacks.extend(callback)
        return self

    def close(self):
        log(INFO, "CLOSING_STREAMBACK")

        if self.feedback_stream:
            self.feedback_stream.close()
        if self.main_stream:
            self.main_stream.close()

    def __repr__(self):
        return "Streamback[name={name},main_stream={main_stream},feedback_stream={feedback_stream}]".format(
            name=self.name,
            main_stream=self.main_stream,
            feedback_stream=self.feedback_stream,
        )
