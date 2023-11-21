class Message(object):
    def __init__(self, streamback, topic, payload, key):
        self.payload = payload
        self.key = key
        self.topic = topic
        self.streamback = streamback
        self.has_responses = False

    @property
    def event(self):
        return self.payload.get("event")

    @property
    def feedback_expected(self):
        return self.payload.get("feedback_expected")

    def respond(self, value=None, payload=None):
        self.streamback.send_feedback(topic=self.feedback_topic, value=value, payload=payload)

    @property
    def value(self):
        return self.payload.get("value")

    @property
    def error(self):
        return self.payload.get("error")

    @property
    def feedback_topic(self):
        return self.payload.get("feedback_topic")

    @property
    def source_group(self):
        return self.payload.get("source_group")

    def __repr__(self):
        return "<Message topic=%s, payload=%s, key=%s>" % (
            self.topic,
            self.payload,
            self.key,
        )


class KafkaMessage(Message):
    def __init__(self, kafka_message, *args, **kwargs):
        super(KafkaMessage, self).__init__(*args, **kwargs)
        self.kafka_message = kafka_message
