import logging
import sys

import os

logger = logging.getLogger("streamback")


def log(level, msg):
    logger.log(level, "(%s)%s" % (os.getpid(), msg))


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj


def reraise_exception(exception):
    if sys.version_info.major >= 3:
        raise exception.with_traceback(exception.__traceback__)
    else:
        raise exception
