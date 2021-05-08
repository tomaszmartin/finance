import logging

FORMAT = "[%(levelname)s %(asctime)s %(module)s:%(funcName)s] %(message)s"
logging.basicConfig(
    format=FORMAT,
    datefmt="%Y.%m.%d %H:%M:%S",
    level=logging.INFO,
)
