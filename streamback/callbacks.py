class Callback(object):

    def on_consume_exception(self, streamback, listener, exception, context, message):
        pass

    def on_consume_begin(self, streamback, listener, context, message):
        pass

    def on_consume_end(self, streamback, listener, context, message, exception=None):
        pass

    def on_fork(self):
        pass

    def on_master_tick(self, streamback):
        pass
