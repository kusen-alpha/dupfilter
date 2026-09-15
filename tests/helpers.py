# -*- coding:utf-8 -*-
import logging


def quiet_logger():
    logger = logging.getLogger("dupfilter-tests")
    logger.setLevel(logging.CRITICAL)
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger
