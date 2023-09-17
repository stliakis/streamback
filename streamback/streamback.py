from logging import INFO, ERROR, WARNING
import uuid

from .context import ConsumerContext
from .events import Events
from .listener import Listener
from .streams import KafkaStream
from .utils import log


class Streamback(object):
    def __init__(self, name, main_stream, feedback_stream=None, feedback_timeout=60, feedback_ttl=300):
        self.name = name

        self.main_stream = main_stream
        self.feedback_stream = feedback_stream

        if self.main_stream:
            self.main_stream.initialize(name)

        if self.feedback_stream:
            self.feedback_stream.initialize(name)

            if isinstance(self.feedback_stream, KafkaStream):
                log(WARNING, "kafka is not recommended for feedback stream, use redis instead")

        self.listeners = {}
        self.routers = []

        self.feedback_timeout = feedback_timeout
        self.feedback_ttl = feedback_ttl

        log(INFO, "STREAMBACK_INITIALIZED[name={name},main_stream={main_stream},feedback_stream={feedback_stream}]".format(
            name=name, main_stream=main_stream, feedback_stream=feedback_stream))

    def get_payload_metadata(self):
        return {
            "source_group": self.name
        }

    def send(
            self, topic, value=None, payload=None, key=None, event=Events.MAIN_STREAM_MESSAGE, flush=True
    ):
        payload = payload or {}

        correlation_id = str(uuid.uuid4())
        feedback_topic = "streamback_feedback_%s" % correlation_id

        payload.update({
            "value": value,
            "event": event,
            "feedback_topic": feedback_topic,
        })

        payload.update(self.get_payload_metadata())

        log(
            INFO,
            "SENDING[topic={topic} key={key} payload={payload}]".format(
                topic=topic, key=key, payload=payload
            ),
        )

        self.main_stream.send(topic, payload, key=key, flush=flush)

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

        payload.update({"event": Events.FEEDBACK_MESSAGE, "source_group": self.name})

        log(
            INFO,
            "SENDING_FEEDBACK[event={event} topic={topic} payload={payload}]".format(
                topic=topic, event=Events.FEEDBACK_MESSAGE, payload=payload
            ),
        )

        self.feedback_stream.send(topic, payload)

    def send_feedback_end(self, topic):
        payload = {"event": Events.FEEDBACK_END}

        payload.update(self.get_payload_metadata())

        log(
            INFO,
            "SENDING_FEEDBACK[event={event} topic={topic} payload={payload}]".format(
                topic=topic,
                event=Events.FEEDBACK_END,
                payload=payload,
            ),
        )

        self.feedback_stream.send(topic, payload)

    def listen(self, topic):
        def decorator(func):
            self.add_listener(Listener(topic=topic, function=func))

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
        for message in self.main_stream.read_stream(streamback=self, topics=topics, timeout=None):
            log(
                INFO,
                "RECEIVED[message={message}]".format(
                    message=message
                ),
            )

            context = ConsumerContext(
                streamback=self
            )

            listeners = self.listeners.get(message.topic, [])
            for listener in listeners:
                listener.function(context, message)
            self.send_feedback_end(message.feedback_topic)

    def add_listener(self, listener):
        self.listeners.setdefault(listener.topic, []).append(
            listener
        )


class FeedbackLane(object):
    def __init__(self, streamback, feedback_topic, feedback_stream):
        self.streamback = streamback
        self.feedback_topic = feedback_topic
        self.feedback_stream = feedback_stream

    def stream(self, from_group, timeout=None):
        self.assert_feedback_stream()

        log(INFO, "LISTENING_FOR_FEEDBACK[feedback_topic={feedback_topic},group={from_group}]".format(
            from_group=from_group,
            feedback_topic=self.feedback_topic))

        for feedback in self.feedback_stream.read_stream(
                streamback=self,
                topics=[self.feedback_topic],
                timeout=timeout or self.streamback.feedback_timeout
        ):
            if from_group and feedback.source_group != from_group:
                continue

            log(INFO, "RECEIVED_FEEDBACK[{feedback}]".format(feedback=feedback))

            if feedback.event == Events.FEEDBACK_END:
                return
            else:
                yield feedback

    def read(self, from_group, timeout=None):
        self.assert_feedback_stream()

        for feedback in self.stream(
                from_group=from_group,
                timeout=timeout
        ):
            if feedback.event == Events.FEEDBACK_MESSAGE:
                return feedback

    def assert_feedback_stream(self):
        if not self.feedback_stream:
            raise Exception("Feedback stream not configured, Streamback is configured as a one way stream")
