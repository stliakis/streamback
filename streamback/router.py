from .listener import Listener


class Router(object):
    listeners = []

    def listen(self, topic, retry=None, input=None, concurrency=None):
        def decorator(func):
            self.listeners.append(
                Listener(topic=topic, function=func, retry_strategy=retry, input=input, concurrency=concurrency)
            )

            def wrapper_func(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper_func

        return decorator
