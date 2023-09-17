import sys

import time

sys.path.append("streamback")

from streamback import Streamback
from router import Router
from streams import KafkaStream, RedisStream

streamback = Streamback(
    "main_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
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

    for i in range(10):
        message.respond({
            "message": "hello back {}".format(i)
        })
        time.sleep(0.1)


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
