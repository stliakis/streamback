## Buslane - 2-way streams for your microservices

### What is a 2-way stream?

----

With Buslane you can implement the producer-consumer model but with a twist. The consumer can
send feedback messages back to the producer via a feedback stream, making it work more like an RPC than the one way stream Kafka is
intended to be used as.


### How it works?

----
Buslane implements two different streams, the main stream and the feedback stream.

- **Main stream**: This is the stream that the producer sends messages to the consumer, it can be Kafka or Redis(or you can
  implement your own stream)
- **Feedback stream**: This is the stream the the consumer sends messages to the producer, Kafka is not recommended for this
  cause each time a new consumer is added to the cluster it causes a rebalance. Redis is the recommended stream for
  this.


### Why not just use the conventional one way streams?

----

Buslane does not stop you from just using the main stream and not sending feedback messages, this way it is behaving just like a Kafka producer-consumer. Buslane just gives
you the option to do so if you need it in order to make more simple the communication between your microservices. 


### Installation

----

```bash
pip install buslane
```



## Examples

### One way stream consumer-producer

#### Consumer

```python
from buslane import Buslane, KafkaStream, RedisStream

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@buslane.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))

buslane.start()
```

#### Producer

```python
from buslane import Buslane, KafkaStream, RedisStream

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

buslane.send("test_hello", "Hello world!")
```

----

### 2-way RPC like communication
#### Consumer

```python
from buslane import Buslane, KafkaStream, RedisStream

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@buslane.listen("test_hello_stream")
def test_hello_stream(context, message):
    print("received: {value}".format(value=message.value))
    message.respond("Hello from the consumer!")

buslane.start()
```

#### Producer

```python
from buslane import Buslane, KafkaStream, RedisStream

buslane = Buslane(
    "example_producer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

message = buslane.send("test_hello_stream", "Hello world!").read(timeout=10)
print(message)
```

---

### 2-way RPC like communication with steaming feedback messages
#### Consumer

```python
from buslane import Buslane, KafkaStream, RedisStream
import time

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

@buslane.listen("test_hello_stream")
def test_hello_stream(context, message):
    print("received: {value}".format(value=message.value))
    for i in range(10):
        message.respond("Hello #{i} from the consumer!".format(i=i))
        time.sleep(2)

buslane.start()
```

#### Producer

```python
from buslane import Buslane, KafkaStream, RedisStream

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

for message in buslane.send("test_hello_stream", "Hello world!").stream():
    print(message)

## OR

stream = buslane.send("test_hello_stream", "Hello world!")

message1 = stream.read()
message2 = stream.read()
message3 = stream.read()
```


## Router
The BuslaneRouter helps with spliting the consumer logic into different files, it is not required to use it but it helps

#### some_consumers.py
```python
from buslane import BuslaneRouter

router = BuslaneRouter()

@router.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))
```


#### my_consumer_app.py
```python
from buslane import Buslane, KafkaStream, RedisStream
from buslane.router import BuslaneRouter

from some_consumers import router as some_consumers_router

buslane = Buslane(
    "example_consumer_app",
    main_stream=KafkaStream("kafka:9092"),
    feedback_stream=RedisStream("redis:6379"),
)

buslane.include_router(some_consumers_router)
```
