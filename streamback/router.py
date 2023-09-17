from .listener import Listener


class Router(object):
    listeners = []

    def listen(self, topic):
        def decorator(func):
            self.listeners.append(
                Listener(topic=topic, function=func)
            )

            def wrapper_func(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper_func

        return decorator
