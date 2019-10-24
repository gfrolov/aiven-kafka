import logging
import logging.config


def create_logger(file_name, log_level):

    logging.config.dictConfig(
        {"version": 1, 'disable_existing_loggers': True})
    logFormatter = logging.Formatter(
        '%(filename)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    if(file_name):
        fileHandler = logging.FileHandler(file_name)
        fileHandler.setFormatter(logFormatter)
        logger.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)

    logger.addHandler(consoleHandler)
    logger.setLevel(log_level)

    return logger
