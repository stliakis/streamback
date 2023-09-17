import logging


def log(level, msg):
    print(level, msg)
    logging.log(level, msg)


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj
