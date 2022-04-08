from typing import Optional
import logging
import logging.config
import os
import datetime
import pytz
from src.core.io.conf_parser import conf

PROJ_PATH = os.environ.get('PROJ_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
LOG_DIR = os.path.join(PROJ_PATH, 'logs')
DEFAULT_LEVEL = logging.DEBUG


class UTCFormatter(logging.Formatter):
    """override `logging.Formatter` to use an aware datetime object"""
    @staticmethod
    def converter(timestamp):
        return datetime.datetime.fromtimestamp(timestamp, tz=pytz.UTC).astimezone(pytz.timezone('Asia/Bangkok'))

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        try:
            return dt.isoformat(timespec='milliseconds')
        except TypeError:
            return dt.isoformat()


class NoConsoleFilter(logging.Filter):
    def __init__(self):
        super().__init__()

    def filter(self, record):
        return not (record.levelname == logging.INFO) & ('no-console' in record.msg)


def setup_log(logging_config: Optional[str] = None):
    """Setup logging with config file"""
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    if os.path.exists(logging_config):
        _log_conf: dict = conf.load(logging_config)
        logging.config.dictConfig(_log_conf)
    else:
        logging.basicConfig(level=DEFAULT_LEVEL)


# def setup_logging(logger=None, logger_name=None, level="DEBUG", clear=True):
#     if logger is None and logger_name is None:
#         raise ValueError("Provide either logger object or logger name")
#     logger = logger or logging.getLogger(logger_name)
#     handle = logging.StreamHandler()
#     formatter = logging.Formatter(
#         "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s -- %(message)s"
#     )
#     handle.setFormatter(formatter)
#     if clear:
#         logger.handlers.clear()
#     logger.addHandler(handle)
#     logger.setLevel(level)
#     return logger


def _create_logger(name):
    return logging.getLogger(name)


def get_logger(name):
    return PickAbleLoggerAdapter(name)


class PickAbleLoggerAdapter(object):
    """pick able logging adapter class"""

    def __init__(self, name):
        self.name = name
        self.logger = _create_logger(name)

    def __getstate__(self):
        """
        Method is called when pickle dumps an object.

        Returns
        -------
        Dictionary, representing the object state to be pickled. Ignores
        the self.logger field and only returns the logger name.
        """
        return {'name': self.name}

    def __setstate__(self, state):
        """
        Method is called when pickle loads an object. Retrieves the name and
        creates a logger.

        Parameters
        ----------
        state - dictionary, containing the logger name.

        """
        self.name = state['name']
        self.logger = _create_logger(self.name)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        self.logger.log(level, msg, *args, **kwargs)


# setup_log(os.path.join(PROJ_PATH, 'conf', 'logging.yaml'))

if __name__ == '__main__':
    setup_log(os.path.join(PROJ_PATH, 'conf', 'logging.yaml'))
    logger = get_logger(__name__)
    logger.debug('debug message')
    logger.debug('filter no-console')
    logger.info('info message')
    logger.warning('warn message')
    logger.error("error message")
    logger.critical('critical message')
