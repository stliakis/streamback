class Callback(object):

    def on_consume_exception(self, streamback, listener, exception, context, message):
        raise NotImplementedError()

    def on_consume_begin(self, streamback, listener, context, message):
        raise NotImplementedError()

    def on_consume_end(self, streamback, listener, context, message, exception=None):
        raise NotImplementedError()
