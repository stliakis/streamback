import sys

import time

from streamback import Streamback, KafkaStream, RedisStream, Listener

streamback = Streamback(
    "main_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)


@streamback.listen("test_streaming")
def test_streaming(context, message):
    for i in range(10):
        message.respond({
            "message": "hello back {}".format(i)
        })
        time.sleep(0.1)


@streamback.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))
    raise Exception()


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
