class RetryStrategy(object):
    retry_times = 0
    retry_interval = 0
    retry_on_exceptions = [Exception]

    def __init__(self, retry_times=None, retry_interval=None, retry_on_exceptions=None):
        self.retry_times = retry_times or self.retry_times
        self.retry_interval = retry_interval or self.retry_interval
        self.retry_on_exceptions = retry_on_exceptions or self.retry_on_exceptions

    def __repr__(self):
        return "<RetryStrategy retry_times=%s, retry_interval=%s, retry_on_exceptions=%s>" % (
            self.retry_times,
            self.retry_interval,
            self.retry_on_exceptions,
        )
