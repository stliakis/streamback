import datetime
import sys
import time

from streamback import Streamback, KafkaStream, RedisStream

#
# streamback = Streamback(
#     "logs",
#     main_stream=KafkaStream(["kafka:9092"]),
#     feedback_stream=RedisStream("redis:6379"),
# )

streamback = Streamback(
    "main_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379&topics_prefix=stefanos-dev-topics",
    log_level="DEBUG"
)

#
# d = {
#     "kSAD ASD ASDA SDSAD-%s" % i: "vAS DASD ASD ASD -%s" % i for i in range(10)
# }

for i in range(1000000):
    streamback.send("new_log", {
        "asdsadsad": "sadsadsad"
    }).flush()
    time.sleep(0.01)

# response = streamback.send("test_input", {
#     "something1": "hello there",
#     "something2": "hello there2"
# }).flush()
#
# streamback.schedule(
#     when="*/30 * * * * *",
#     then="carzilla.send_grafana_stats",
#     args={
#         "something1": "hello there",
#     },
#     description="test the schedule blabla bla"
# )
#
# streamback.schedule(
#     when="23 0 10 * * *",
#     then="carzilla.send_emails_to_new_users",
#     args={
#         "something1": "hello there",
#     },
#     description="Send emails at 10 to new users"
# )
#
# streamback.start()


# class Response(object):
#     def __init__(self, message):
#         self.message = message
#
#
# response = streamback.send("test_map", {
#     "test_key1": "hello there",
#     "test_key2": "hello there2"
# })

# print(response)

# for message in streamback.send("test_streaming", {
#     "message": "hello there"
# }).stream("logs"):
#     print(message)

# streamback = Streamback(
#     "main_app",
#     streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
# )
#
# stream = streamback.send("test_streaming", {"message": "hello there"})
#
# message1 = stream.read(from_group="main_app")
# message2 = stream.read(from_group="main_app")
#
# ## You can even safe the feedback_topic and continue the stream later
# stream = streamback.continue_stream(stream.feedback_topic)
# message3 = stream.read(from_group="main_app")
#
# print("message1:", message1)
# print("message2:", message2)
# print("message3:", message3)
