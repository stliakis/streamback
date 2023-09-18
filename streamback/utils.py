import logging

logging.basicConfig(
    format='streamback - %(levelname)s - %(message)s  ',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger("streamback")

def log(level, msg):
    logger.log(level, msg)


def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    return obj
