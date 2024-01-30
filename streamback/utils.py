import logging
import sys

import os

logger = logging.getLogger("streamback")


def log(level, msg):
    logger.log(level, "[p=%s]%s" % (os.getpid(), msg))


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj


def reraise_exception(exception):
    if sys.version_info.major >= 3:
        raise exception.with_traceback(exception.__traceback__)
    else:
        raise exception


def bytes_to_pretty_string(bytes):
    if bytes < 1024:
        return "{} B".format(bytes)
    elif bytes < 1024 * 1024:
        return "{:.2f} KB".format(bytes / 1024)
    elif bytes < 1024 * 1024 * 1024:
        return "{:.2f} MB".format(bytes / (1024 * 1024))
    else:
        return "{:.2f} GB".format(bytes / (1024 * 1024 * 1024))
