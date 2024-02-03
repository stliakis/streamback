import inspect

from .listener import Listener


class Router(object):
    listeners = []

    def listen(self, topic, retry=None, input=None, concurrency=None):
        def decorator(func_or_listener_class):
            if inspect.isclass(func_or_listener_class) and issubclass(func_or_listener_class, Listener):
                self.listeners.append(
                    func_or_listener_class(topic=topic, retry_strategy=retry, input=input, concurrency=concurrency)
                )
            else:
                self.listeners.append(
                    Listener(topic=topic, function=func_or_listener_class, retry_strategy=retry, input=input,
                             concurrency=concurrency)
                )

        return decorator
