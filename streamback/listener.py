class Listener(object):
    topic = None
    function = None

    def __init__(self, topic=None, function=None):
        self.topic = topic or self.topic
        self.function = function or self.function

    def consume(self, context, message):
        if self.function:
            self.function(context, message)
