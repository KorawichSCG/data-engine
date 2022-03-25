import os
import datetime
import yaml
import re
import time
import pytz
import requests
import json
import logging
from logging.config import dictConfig
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
LOG_DIR = os.path.join(ROOT, 'log')
DEBUG = eval(os.environ.get("DEBUG", "False"))
LOG_CONF = eval(os.environ.get("LOG_CONF", "False"))

if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


class Message:
    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return self.fmt.format(*self.args)


class StyleAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, Message(msg, args), (), **kwargs)


def constructor_app_path(loader, node):
    value = loader.construct_scalar(node)
    return os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)), value)


def setup_logger(output_file=None, logging_config=None):
    # logging_config must be a dictionary object specifying the configuration
    if output_file and not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))

    # if logging_config:
    #     if output_file:
    #         logging_config['handlers']['file_handler']['filename'] = output_file
    #     logging.config.dictConfig(logging_config)

    else:
        yaml.add_constructor(u'!appPath', constructor_app_path)
        conf_file = os.path.join(
            os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)), 'conf.test', 'logging.json'
        )
        with open(conf_file, 'r') as file:
            # logging_config = yaml.load(files, Loader=yaml.SafeLoader)
            logging_config = json.loads(file.read())

        # if output_file is not None:
        #     logging_config['handlers']['file_handler']['filename'] = output_file

        logging.config.dictConfig(logging_config)
        del logging_config


def _create_logger(name):
    return StyleAdapter(logging.getLogger(name))


def get_logger(name):
    return PickableLoggerAdapter(name)


class PickableLoggerAdapter(object):
    """
    pickle module
    """
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

    def isEnabledFor(self, level):
        return self.logger.isEnabledFor(level)

setup_logger()
