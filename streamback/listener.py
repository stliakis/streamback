import time
from logging import INFO

from .utils import log
from .retry_strategy import RetryStrategy


class Listener(object):
    topic = None
    function = None
    retry_strategy = RetryStrategy()

    def __init__(self, topic=None, function=None, retry_strategy=None):
        self.topic = topic or self.topic
        self.function = function or self.function
        self.retry_strategy = retry_strategy or self.retry_strategy

    def try_to_consume(self, context, message, retry_times=0):
        try:
            self.consume(context, message)
        except Exception as e:
            log(
                INFO,
                "CONSUME_EXCEPTION[exception={e}, retry_strategy={retry_strategy}, retry_times={retry_times}]".format(
                    e=e, retry_strategy=self.retry_strategy, retry_times=retry_times
                ),
            )

            if not isinstance(e, tuple(self.retry_strategy.retry_on_exceptions)):
                raise e

            if self.retry_strategy.retry_times > 0:
                if retry_times >= self.retry_strategy.retry_times:
                    log(
                        INFO,
                        "MAX_RETRIES_REACHED[message={message}, retry_times={retry_times}]".format(message=message,
                                                                                                   retry_times=retry_times),
                    )
                    raise e
                else:
                    if self.retry_strategy.retry_interval:
                        time.sleep(self.retry_strategy.retry_interval)
                    log(
                        INFO,
                        "RETRYING_FAILED_MESSAGE[message={message}, retry_times={retry_times}]".format(message=message,
                                                                                                       retry_times=retry_times),
                    )
                    self.try_to_consume(context, message, retry_times=retry_times + 1)
            else:
                raise e

    def consume(self, context, message):
        if self.function:
            self.function(context, message)
