class FeedbackTimeout(Exception):
    def __init__(self, topics, timeout):
        self.topics = topics
        self.timeout = timeout

    def __str__(self):
        return "FeedbackTimeout[topics=%s, timeout=%s]" % (
            self.topics,
            self.timeout,
        )


class InvalidSteamsString(Exception):
    def __init__(self, streams_string, message=None):
        self.streams_string = streams_string
        self.message = message

    def __str__(self):
        return "InvalidSteamsString[streams_string=%s,error=%s]" % (
            self.streams_string, self.message
        )
