import logging

logger = logging.getLogger("streamback")

def log(level, msg):
    logger.log(level, msg)


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj
