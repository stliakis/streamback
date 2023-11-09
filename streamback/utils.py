import logging

logger = logging.getLogger("streamback")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
formatter = logging.Formatter('streamback - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def log(level, msg):
    logger.log(level, msg)


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj
