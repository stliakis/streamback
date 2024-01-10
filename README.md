## 2-way streams for your microservices

### What is a stream with feedbacks?

----

With Streamback you can implement the producer-consumer model but with a twist. The consumer can
send feedback messages back to the producer via a feedback stream, making it work more like an RPC than the one way
stream Kafka is intended to be used as.

### How it works?

----
Streamback implements two different streams, the main stream and the feedback stream.

- **Main stream**: This is the kafka stream that the producer sends messages to the consumer.
- **Feedback stream**: This is the stream that the consumer sends messages to the producer. Redis is used for this stream for its
- simplicity and speed.

### Why not just use the conventional one way streams?

----

Streamback does not stop you from just using the main stream and not sending feedback messages, this way it is behaving
just like a Kafka producer-consumer. Streamback just gives
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
from streamback import Streamback

streamback = Streamback(
    "example_consumer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)


@streamback.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))


streamback.start()
```

#### Producer

```python
from streamback import Streamback

streamback = Streamback(
    "example_producer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)

streamback.send("test_hello", {"something":"Hello world!"})
```

----

### 2-way RPC like communication

#### Consumer

```python
from streamback import Streamback

streamback = Streamback(
    "example_consumer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)


@streamback.listen("test_hello_stream")
def test_hello_stream(context, message):
    print("received: {value}".format(value=message.value))
    message.respond("Hello from the consumer!")


streamback.start()
```

#### Producer

```python
from streamback import Streamback

streamback = Streamback(
    "example_producer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)

message = streamback.send("test_hello_stream", {"something":"Hello world!"}).read(timeout=10)
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
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
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
from streamback import Streamback

streamback = Streamback(
    "example_producer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)

for message in streamback.send("test_hello_stream", {"something":"Hello world!"}).stream():
    print(message)

## OR

stream = streamback.send("test_hello_stream", {"something":"Hello world!"})

message1 = stream.read()
message2 = stream.read()
message3 = stream.read()
```

----

## Consumer input mapping to objects

For a more type oriented approach you can map the input of the consumer to a class.

```python
class TestInput(object):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

@streamback.listen("test_input")
def test_input(context, message):
    input = message.map(TestInput)
    print(input.arg1)
    print(input.arg2)
    message.respond({
        "arg1": input.arg1,
        "arg2": input.arg2
    })
```

## Producer feedback mapping to objects

In a similar way you can map the feedback of the producer to a class.

```python
class TestResponse(object):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2


response = streamback.send("test_input", {"arg1": "Hello world!", "arg2": "Hello world!"}).read("main_app",
                                                                                                map=TestResponse)
print(response.arg1)
print(response.arg2)
```

## Input injection

Instead of having to deconstruct the message.value inside the consumer's logic, you can pass
to the consumer only the arguments of the message.value that you want to use.


```python
@streamback.listen("test_input", input = ["arg1", "arg2"])
def test_input(arg1, arg2):
    pass

streamback.send("test_input", {"arg1":"Hello world!", "arg2": "Hello world!"})
```

### Class based consumers

```python
@streamback.listen("new_log")
class LogsConsumer(Listener):
    logs = []

    def consume(self, context, message):
        self.logs.append(message.value)
        if len(self.logs) > 100:
            self.flush_logs()

    def flush_logs(self):
        database_commit(self.logs)
```

## Router

The StreambackRouter helps with spliting the consumer logic into different files, it is not required to use it but it
helps

#### some_consumers.py

```python
from streamback import Router

router = Router()


@router.listen("test_hello")
def test_hello(context, message):
    print("received: {value}".format(value=message.value))
```

#### my_consumer_app.py

```python
from streamback import Streamback

from some_consumers import router as some_consumers_router

streamback = Streamback(
    "example_consumer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379"
)

streamback.include_router(some_consumers_router)

streamback.start()
```

## Handling consume exceptions
You can pass the on_exception callback upon creating the Streamback object to handle exceptions that occur during the consumption of messages by the listeners

```python

from streamback import Streamback

def on_exception(listener, context, message, exception):
    print("on_exception:", listener, context, message, exception)

streamback = Streamback(
    "example_consumer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379",
    on_exception=on_exception
)
```

```python

from streamback import Streamback

def on_exception(listener, context, message, exception):
    print("on_exception:", listener, context, message, exception)

streamback = Streamback(
    "example_consumer_app",
    streams="main=kafka://kafka:9092&feedback=redis://redis:6379",
    on_exception=on_exception
)
```


### Why python 2.7 compatible?

Streamback has been created for usage in car.gr's systems which has some legacy python 2.7 services. We are are planing
to move Streamback to python >3.7 in some later version but for now the python 2.7 support was crucial and thus the
async/await support was sacrificed.