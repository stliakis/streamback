import inspect
import logging
import time
import traceback
from logging import INFO, ERROR, WARNING, DEBUG
import uuid
import signal
import sys
import psutil

from .scheduler import Scheduler, SchedulerState
from .process_manager import ListenersRunner, TopicProcessMessages
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
            topics=None,
            topics_prefix=None,
            main_stream=None,
            feedback_stream=None,
            feedback_timeout=60,
            feedback_ttl=300,
            main_stream_timeout=5,
            auto_flush_messages_count=200,
            rescale_interval=1,
            rescale_min_process_ttl=10,
            rescale_max_memory_mb=None,
            pool_concurrency=None,
            on_exception=None,
            callbacks=None,
            extensions=None,
            retry_strategy=None,
            scheduler_state_file="streamback_scheduler_state.json",
            scheduler_keep_state_ttl=3600,
            log_level="INFO",
            **kwargs
    ):
        self.initialize_logger(log_level)

        self.extensions = extensions or callbacks or []
        self.name = name
        self.manager_name = "streamback_manager_%s" % name
        self.feedback_timeout = feedback_timeout
        self.feedback_ttl = feedback_ttl
        self.on_exception = on_exception
        self.topics_prefix = topics_prefix
        self.main_stream_timeout = main_stream_timeout
        self.rescale_interval = rescale_interval
        self.default_retry_strategy = retry_strategy
        self.auto_flush_messages_count = auto_flush_messages_count
        self.rescale_min_process_ttl = rescale_min_process_ttl
        self.rescale_max_memory_mb = rescale_max_memory_mb
        self.pool_concurrency = pool_concurrency or [[0, 1]]
        self.scheduler = Scheduler(
            state=SchedulerState(
                filepath=scheduler_state_file,
                state_ttl=scheduler_keep_state_ttl
            )
        )

        if streams:
            parsed_streams = ParsedStreams(name, streams)
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

        self.listeners = []
        self.routers = []
        self.process_manager = None

        self.listen_only_for_topics = [self.get_topic_real_name(topic) for topic in topics or []]

    def initialize_main_stream(self):
        if self.main_stream and not self.main_stream.is_initialized():
            self.main_stream.initialize(
                flush_timeout=self.main_stream_timeout,
                auto_flush_messages_count=self.auto_flush_messages_count
            )

    def get_current_memory_usage(self):
        total_rss = 0
        for topic_process in self.process_manager.get_processes():
            process = psutil.Process(topic_process.process.pid)
            memory_info = process.memory_info()
            total_rss += memory_info.rss

        return total_rss

    def initialize_feedback_stream(self):
        if self.feedback_stream and not self.feedback_stream.is_initialized():
            self.feedback_stream.initialize()

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
        for extension in self.get_extensions():
            extension.on_fork()

    def get_payload_metadata(self):
        return {"source_group": self.name}

    def flush(self):
        if self.feedback_stream and self.feedback_stream.is_initialized():
            self.feedback_stream.flush()

        if self.main_stream and self.main_stream.is_initialized():
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

    def on_message_from_master_process(self, message, topics, listeners):
        log(INFO, "RECEIVED_MESSAGE_FROM_MASTER_PROCESS[message={message}]".format(message=message))
        if message == TopicProcessMessages.TERMINATE:
            log(INFO, "LISTENER_PROCESS_KILLED[topics={topics}]".format(topics=topics))
            self.close(listeners, reason="received terminate from master process")
            sys.exit(0)

    def start_listeners(self, pipe, topics, listeners):
        self.listeners = listeners
        self.initialize_streams()
        self.on_fork()

        def on_tick():
            if pipe.poll():
                self.on_message_from_master_process(pipe.recv(), topics, listeners)

        for message in self.main_stream.read_stream(
                streamback=self, topics=topics, timeout=None, on_tick=on_tick
        ):
            log(
                DEBUG,
                "RECEIVED[message={message}]".format(message=message),
            )

            context = ConsumerContext(streamback=self)

            failed_listeners = []
            for listener in listeners:
                if message.topic not in listener.topics:
                    continue

                try:
                    for extension in self.get_extensions():
                        extension.on_consume_begin(self, listener, context, message)
                    listener.try_to_consume(context, message)
                    for extension in self.get_extensions():
                        extension.on_consume_end(self, listener, context, message)
                except Exception as e:
                    log(
                        ERROR,
                        "LISTENER_ERROR[listener={listener} error={error}]".format(
                            listener=listener, error=str(e)
                        ),
                    )
                    traceback.print_exc()
                    failed_listeners.append([listener, e, context, message])
                    for extension in self.get_extensions():
                        extension.on_consume_end(
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

    def _start_listener(self, pipe, topics, listeners):
        def signal_handler(sig, frame):
            log(INFO, "LISTENER_PROCESS_KILLED[topics={topics}]".format(topics=topics))
            self.close(listeners, reason="listener process killed")
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            self.start_listeners(pipe, topics, listeners)
        except KeyboardInterrupt as ex:
            self.close(listeners,
                       reason="keyboard kill command")
        except Exception as ex:
            self.close(listeners,
                       reason="exception while starting listeners[exception={exception}]".format(exception=ex))

    def start(self):
        self.start_listeners_runner()
        self.start_scheduler()

        try:
            while True:
                self.process_manager.on_tick()
                self.scheduler.execute(self)

                for extension in self.get_extensions():
                    try:
                        extension.on_master_tick(self)
                    except Exception as ex:
                        log(
                            ERROR,
                            "EXTENSION_ERROR[extension={extension},exception={exception}]".format(
                                extension=extension, exception=ex
                            ),
                        )
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def start_listeners_runner(self):
        self.process_manager = ListenersRunner(self, self.listeners, self._start_listener)
        self.process_manager.spin()

    def start_scheduler(self):
        log(INFO, "STREAMBACK_SCHEDULER_STARTING[tasks=[{tasks}]]".format(tasks=self.scheduler.tasks))

    def get_extensions(self):
        return self.extensions

    def consume_exception(self, listener, exception, context, message):
        if self.on_exception:
            self.on_exception(listener, context, message, exception)
        for extension in self.get_extensions():
            extension.on_consume_exception(self, listener, exception, context, message)

    def add_listener(self, listener):
        listener.topics = [self.get_topic_real_name(topic) for topic in listener.topics]
        self.listeners.append(listener)
        return self

    def add_callback(self, *callback):
        return self.extend(*callback)

    def extend(self, *extension):
        self.extensions.extend(extension)
        return self

    def schedule(self, name, when, then, args=None, description=None):
        self.scheduler.add_task(
            name=name,
            when=when,
            then=then,
            args=args,
            description=description
        )

    def close(self, listeners, reason=None):
        for listener in listeners:
            log(INFO, "FLUSHING_LISTENER[listener={listener}]".format(listener=listener))
            listener.flush()

        if self.feedback_stream and self.feedback_stream.is_initialized():
            self.feedback_stream.close()
        if self.main_stream and self.main_stream.is_initialized():
            self.main_stream.close()

        log(INFO, "CLOSING_STREAMBACK[reason={reason}]".format(reason=reason))

    def __repr__(self):
        return "Streamback[name={name},main_stream={main_stream},feedback_stream={feedback_stream}]".format(
            name=self.name,
            main_stream=self.main_stream,
            feedback_stream=self.feedback_stream,
        )
