import math
import sys

import time

from streamback.extensions.auto_restart import AutoRestart
from streamback.retry_strategy import RetryStrategy
from streamback import Streamback, KafkaStream, RedisStream, Listener, Callback, ListenerStats, Router


def on_exception(listener, context, message, exception):
    print("on_exception:", type(exception))


class StreambackCallbacks(Callback):
    # def on_consume_begin(self, streamback, listener, context, message):
    #     print("on_consume_begin:", message)
    #
    # def on_consume_end(self, streamback, listener, context, message, exception=None):
    #     print("on_consume_end:", message, exception)
    #
    # def on_consume_exception(self, streamback, listener, exception, context, message):
    #     print("on_consume_exception:", type(exception))

    # def on_master_tick(self, process_manager):
    #     print("on maste tick")
    #     process_manager.send_message_to_all_processes(TopicProcessMessages.TERMINATE)

    def on_fork(self):
        print("on_fork")


streamback = Streamback(
    "main_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379&topics_prefix=stefanos-dev-topics",
    on_exception=on_exception,
    log_level="DEBUG",
    rescale_max_memory_mb=1000
).add_callback(StreambackCallbacks(), ListenerStats(5), AutoRestart(max_seconds=1000, max_memory_mb=100))


# #
# @streamback.listen("test_input", input=["something1", "something2"], concurrency=1)
# def test_input(something1, something2):
#     print("test_input:", something1, something2)
#
#
# @streamback.listen("test_input", input=["something1", "something2"], concurrency=1)
# def test_input2(something1, something2):
#     print("test_input2:", something1, something2)


#
# @streamback.listen("test_streaming")
# def test_streaming(context, message):
#     for i in range(10):
#         message.respond({"message": "hello back {}".format(i)})
#         time.sleep(0.1)
# @streamback.listen("test_hello2", retry=RetryStrategy(retry_times=3), concurrency=2)
# def test_hello(context, message):
#     for i in range(1000):
#         a = math.sqrt(i)
#
#
# @streamback.listen("test_hello", retry=RetryStrategy(retry_times=3), concurrency=2)
# def test_hello(context, message):
#     for i in range(1000):
#         a = math.sqrt(i)

# print("received: {value}".format(value=message.value))
# message.respond({
#     "message": "hello back"
# })
# pass
# a = 1 / 0


#
# @streamback.listen("test_map")
# def test_map(message):
#     class MessageValue(object):
#         def __init__(self, test_key1, test_key2):
#             self.test_key1 = test_key1
#             self.test_key2 = test_key2
#
#     input = message.map(MessageValue)
#
#     print("input:", input, input.test_key1, input.test_key2)
#
#     message.respond({"message": "hello back"})
#
# #
router = Router()



@router.listen("new_log3", concurrency=[[10, 2], [20, 4], [40000, 5], [50000, 7]])
class LogsConsumer(Listener):
    logs = []

    def consume(self, context, message):
        print("consume")
        pass
        # self.logs.append(message.value)
        # if len(self.logs) > 100:
        #     self.flush()
        # time.sleep(0.5)

    # def flush(self):
    #     print("flush called")
    #     time.sleep(10)
    #     print("flushed///")
    #     pass
    #     database_commit(self.logs)



streamback.include_router(router)

@streamback.listen("new_log", concurrency=[0, 10])
# @streamback.listen("new_log", concurrency=[[0, 0], [10, 10]])
def test_receiver2(message):
    time.sleep(0.1)
    # print("test_receiver2:", message)

@streamback.listen("new_log2", concurrency=1)
# @streamback.listen("new_log", concurrency=[[0, 0], [10, 10]])
def test_receiver2(message):
    time.sleep(0.1)
#
streamback.start()
