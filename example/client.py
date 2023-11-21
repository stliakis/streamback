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

streamback.send("test_hello", {
    "message": "hello"
}).flush()

# response = streamback.send("test_streaming", {
#     "message": "hello there"
# }).read("logs",0.001)
#
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
