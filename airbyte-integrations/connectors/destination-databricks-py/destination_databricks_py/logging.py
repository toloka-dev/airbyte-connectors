import logging

import dbxio

LOGGER = logging.getLogger("airbyte")
__LOGGING_INITED = False


def init_logging(debug: bool) -> None:
    global __LOGGING_INITED
    if __LOGGING_INITED:
        return

    LOGGER.setLevel(logging.DEBUG if debug else logging.INFO)

    dbxio_logger = dbxio.get_logger()
    for h in LOGGER.handlers:
        dbxio_logger.addHandler(h)

    dbxio_logger.setLevel(LOGGER.level)
    __LOGGING_INITED = True
