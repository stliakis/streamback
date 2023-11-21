import logging
import sys

logger = logging.getLogger("streamback")


def log(level, msg):
    logger.log(level, msg)


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj


def reraise_exception(exception):
    if sys.version_info.major >= 3:
        raise exception.with_traceback(exception.__traceback__)
    else:
        raise exception
