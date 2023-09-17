
class FeedbackTimeout(Exception):
    def __init__(self, topics, timeout):
        self.topics = topics
        self.timeout = timeout

    def __str__(self):
        return "FeedbackTimeout[topics=%s, timeout=%s]" % (
            self.topics,
            self.timeout,
        )
