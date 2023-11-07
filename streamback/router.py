from .listener import Listener


class Router(object):
    listeners = []

    def listen(self, topic, retry=None):
        def decorator(func):
            self.listeners.append(
                Listener(topic=topic, function=func, retry_strategy=retry)
            )

            def wrapper_func(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper_func

        return decorator
