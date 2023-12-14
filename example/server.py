import sys

import time

from streamback.retry_strategy import RetryStrategy
from streamback import Streamback, KafkaStream, RedisStream, Listener, Callback


def on_exception(listener, context, message, exception):
    print("on_exception:", type(exception))


streamback = Streamback(
    "main_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379&topics_prefix=stefanos-dev-topics",
    on_exception=on_exception,
    log_level="DEBUG"
)


class StreambackCallbacks(Callback):
    def on_consume_begin(self, streamback, listener, context, message):
        print("on_consume_begin:", message)

    def on_consume_end(self, streamback, listener, context, message, exception=None):
        print("on_consume_end:", message, exception)

    def on_consume_exception(self, streamback, listener, exception, context, message):
        print("on_consume_exception:", type(exception))


streamback.add_callback(StreambackCallbacks())


@streamback.listen("test_input", input=["something1", "something2"])
def test_input(something1, something2):
    print("test_input:", something1, something2)


@streamback.listen("test_streaming")
def test_streaming(context, message):
    for i in range(10):
        message.respond({
            "message": "hello back {}".format(i)
        })
        time.sleep(0.1)


@streamback.listen("test_hello", retry=RetryStrategy(retry_times=3))
def test_hello(context, message):
    # print("received: {value}".format(value=message.value))
    # message.respond({
    #     "message": "hello back"
    # })
    pass
    # a = 1 / 0


@streamback.listen("new_log")
class LogsConsumer(Listener):
    logs = []

    def consume(self, context, message):
        self.logs.append(message.value)
        if len(self.logs) > 100:
            self.flush_logs()

    def flush_logs(self):
        pass
        # database_commit(self.logs)


# router = Router()


#
# streamback.include_router(router)
#
# # @streamback.receive("log_delete")
# # def test_receiver2(message):
# #     print("test_receiver2:", message)
#
#
streamback.start()
