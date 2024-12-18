import sys
import time
from logging import INFO

import inspect

from .utils import log, listify
from .retry_strategy import RetryStrategy


class Listener(object):
    function = None
    input = None
    retry_strategy = RetryStrategy()

    def __init__(self, topic=None, function=None, retry_strategy=None, input=None, concurrency=None):
        self.topics = listify(topic or self.topic)

        self.input = input or self.input
        self.function = function or self.function
        self.retry_strategy = retry_strategy or self.retry_strategy
        self.concurrency = concurrency

    @property
    def topic(self):
        return self.topics[0]

    def get_listener_function_valid_arguments(self, func):
        if sys.version_info.major == 2:
            args, varargs, keywords, defaults = inspect.getargspec(func)
            return args
        else:
            return list(inspect.signature(func).parameters.keys())

    def try_to_consume(self, context, message, retry_times=0):
        try:
            if self.input:
                self.consume_input(
                    **{key: message.value.get(key) for key in self.input}
                )
            else:
                self.consume(context, message)
        except Exception as e:
            log(
                INFO,
                "CONSUME_EXCEPTION[exception={e}, retry_strategy={retry_strategy}, retry_times={retry_times}]".format(
                    e=e, retry_strategy=self.retry_strategy, retry_times=retry_times
                ),
            )

            if not isinstance(e, tuple(self.retry_strategy.retry_on_exceptions)):
                raise

            if self.retry_strategy.retry_times > 0:
                if retry_times >= self.retry_strategy.retry_times:
                    log(
                        INFO,
                        "MAX_RETRIES_REACHED[message={message}, retry_times={retry_times}]".format(
                            message=message, retry_times=retry_times
                        ),
                    )
                    raise
                else:
                    if self.retry_strategy.retry_interval:
                        time.sleep(self.retry_strategy.retry_interval)
                    log(
                        INFO,
                        "RETRYING_FAILED_MESSAGE[message={message}, retry_times={retry_times}]".format(
                            message=message, retry_times=retry_times
                        ),
                    )
                    self.try_to_consume(context, message, retry_times=retry_times + 1)
            else:
                raise

    def consume(self, context, message):
        if self.function:
            valid_arguments = self.get_listener_function_valid_arguments(self.function)

            args = []
            if "context" in valid_arguments:
                args.append(context)
            args.append(message)

            self.function(*args)

    def flush(self, *args, **kwargs):
        pass

    def consume_input(self, *args, **kwargs):
        if self.function:
            self.function(*args, **kwargs)

    def __repr__(self):
        if self.function:
            return "Listener[topics=%s,function=%s]" % (self.topics, self.function.__name__)
        else:
            return "Listener[topics=%s,name=%s]" % (self.topics, self.__class__.__name__)
