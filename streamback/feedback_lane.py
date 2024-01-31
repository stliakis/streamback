from logging import INFO, DEBUG

from .events import Events
from .exceptions import CouldNotFlushToMainStream, FeedbackError
from .utils import log


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
