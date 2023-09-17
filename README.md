## 2-way streams for your microservices

### What is a 2-way stream?

----

With Streamback you can implement the producer-consumer model but with a twist. The consumer can
send feedback messages back to the producer via a feedback stream, making it work more like an RPC than the one way stream Kafka is
intended to be used as.


### How it works?

----
Streamback implements two different streams, the main stream and the feedback stream.

- **Main stream**: This is the stream that the producer sends messages to the consumer, it can be Kafka or Redis(or you can
  implement your own stream)
- **Feedback stream**: This is the stream the the consumer sends messages to the producer, Kafka is not recommended for this
  cause each time a new consumer is added to the cluster it causes a rebalance. Redis is the recommended stream for
  this.


### Why not just use the conventional one way streams?

----

Streamback does not stop you from just using the main stream and not sending feedback messages, this way it is behaving just like a Kafka producer-consumer. Streamback just gives
you the option to do so if you need it in order to make more simple the communication between your microservices. 


### Installation

----

```bash
pip install streamback
```

## Examples

### One way stream consumer-producer

#### Consumer

```python
from streamback import Streamback, KafkaStream, RedisStream

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@streamback.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))

streamback.start()
```

#### Producer

```python
from streamback import Streamback, KafkaStream, RedisStream

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

streamback.send("test_hello", "Hello world!")
```

----

### 2-way RPC like communication
#### Consumer

```python
from streamback import Streamback, KafkaStream, RedisStream

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@streamback.listen("test_hello_stream")
def test_hello_stream(context, message):
    print("received: {value}".format(value=message.value))
    message.respond("Hello from the consumer!")

streamback.start()
```

#### Producer

```python
from streamback import Streamback, KafkaStream, RedisStream

streamback = Streamback(
    "example_producer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

message = Streamback.send("test_hello_stream", "Hello world!").read(timeout=10)
print(message)
```

---

### 2-way RPC like communication with steaming feedback messages
#### Consumer

```python
from streamback import Streamback, KafkaStream, RedisStream
import time

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@streamback.listen("test_hello_stream")
def test_hello_stream(context, message):
    print("received: {value}".format(value=message.value))
    for i in range(10):
        message.respond("Hello #{i} from the consumer!".format(i=i))
        time.sleep(2)

streamback.start()
```

#### Producer

```python
from streamback import Streamback, KafkaStream, RedisStream

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

for message in Streamback.send("test_hello_stream", "Hello world!").stream():
    print(message)

## OR

stream = Streamback.send("test_hello_stream", "Hello world!")

message1 = stream.read()
message2 = stream.read()
message3 = stream.read()
```


## Router
The StreambackRouter helps with spliting the consumer logic into different files, it is not required to use it but it helps

#### some_consumers.py
```python
from streamback import StreambackRouter

router = StreambackRouter()

@router.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))
```


#### my_consumer_app.py
```python
from streamback import Streamback, KafkaStream, RedisStream
from streamback.router import StreambackRouter

from some_consumers import router as some_consumers_router

streamback = Streamback(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

streamback.include_router(some_consumers_router)
```
