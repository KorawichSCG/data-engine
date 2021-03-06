import datetime
import os
import gzip
import json
import sys
import logging
from typing import (
    Optional, Dict, Any, List, Union, Callable, Type, Literal, NoReturn, NewType, ClassVar
)
from src.core.errors import (
    ValidateSchemaError, ConfigNotFound, ValidateTypeError
)
from src.core.utils import path_join, merge_dicts, import_string
from src.core.io import parse_config, load_dotenv
from src.core.io.database import postgresql
from src.core.io.storage import local
from pathlib import Path


logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s"))
logger.addHandler(consoleHandler)
logger.setLevel('INFO')

os.environ.setdefault('PROJ_PATH', path_join(Path(__file__).parent, '../../..'))
load_dotenv(path_join(os.environ['PROJ_PATH'], 'conf'))


class ConfigParser:
    """
    Create config instance from `yaml` configuration file after convert to
    compressed `json` file. This class will validate structure of config keys
    """
    CONF_SUB_PATH: ClassVar[str] = 'defaults'
    CLASS_VALIDATE: ClassVar[List[object]] = []
    CONF_PATH = path_join(os.environ['PROJ_PATH'], os.environ.get('CONF_PATH', 'conf'))
    DATA_PATH = path_join(os.environ['PROJ_PATH'], os.environ.get('DATA_PATH', 'data'))
    PROJ_ENV = os.environ.get('PROJ_ENV', 'sandbox')

    def __init__(
            self,
            conf_name: str,
            conf_file_prefix: Optional[str] = None,
            conf_file_suffix: Optional[str] = None,
            conf_cache_time: Optional[Dict[str, Any]] = None,
            conf_map_type: str = 'type',
            conf_schema: str = 'conf_schemas',
            conf_file_suffix_extend: str = '.',
            conf_compress: str = 'gzip',
            verbose: bool = True
    ):
        """
        Base argument for parser `yaml` config will be get
        :param: conf_name - Configuration key name in `yaml` file
        :param: conf_file_prefix - Prefix `yaml` file name
        :param: conf_file_suffix - Suffix `yaml` file name
        """
        self.conf_name: str = conf_name
        self.conf_file_prefix: str = conf_file_prefix or ''
        self.conf_file_suffix: str = f'{conf_file_suffix_extend}{conf_file_suffix}' if conf_file_suffix else ''
        self.conf_cache_time: Dict[str, Any] = conf_cache_time or {'seconds': 10}
        self.conf_schema: str = conf_schema
        self.conf_map_type: str = conf_map_type
        self.conf_compress: str = conf_compress
        self.verbose: bool = verbose

        if not self.validate_schemas:
            raise ValidateSchemaError(
                f'Config `{self.CONF_SUB_PATH}` must have keys in '
                f'{", ".join((f"`{_}`" for _ in set(self.conf_schemas.keys()) - set(self.conf_data.keys())))}'
            )
        elif not self.conf_data:
            raise ConfigNotFound(f'{self.conf_name!r} does not exists in {self.CONF_SUB_PATH!r}')
        elif not self.validate_class:
            raise ValidateTypeError(f'Config type does not set for {self.conf_type}')
        elif self.verbose:
            logger.info("Process of config validation is successful")

    def __repr__(self):
        """Overrides the default `__repr__` implementation"""
        return f'{self.__class__.__name__}(conf_name={self.conf_name!r})'

    def __eq__(self, other):
        """Overrides the default `__eq__` implementation"""
        if isinstance(other, ConfigParser):
            return self.conf_name == other.conf_name
        return False

    @property
    def conf_cache_name(self) -> str:
        """Create cache name with compress format `.json.gz`."""
        return f'.cache.{self.CONF_SUB_PATH}.{self.conf_file_prefix}{self.conf_file_suffix}.json.gz'

    @property
    def conf_cache_path(self) -> Path:
        """Create cache path for persisted to `DATA_PATH`"""
        return Path(os.path.join(self.DATA_PATH, self.PROJ_ENV, 'conf', self.conf_cache_name))

    @property
    def conf_data(self) -> Dict[str, Any]:
        """
        Property of `get_conf_cache` method
        """
        return self.get_config_cache(self.conf_name)

    @property
    def conf_schemas(self) -> Dict[str, Any]:
        """
        configuration schemas
        """
        _conf_schema: Dict = self.get_config(self.conf_schema).pop(self.CONF_SUB_PATH, {})
        return _conf_schema.get(f'{self.conf_file_prefix}{self.conf_file_suffix}', {}) or _conf_schema.get(
            self.conf_file_prefix, {}
        )

    @property
    def conf_type(self):
        return self.conf_data.get(self.conf_map_type, '')

    def put_config_cache(
            self,
            date_format: str = '%Y-%m-%d %H:%M:%S',
            encoding: str = 'utf-8'
    ) -> NoReturn:
        """
        Convert configuration data from `yaml` to `json` file
        """
        if self.verbose:
            logger.info(f"Write config `json` file to {self.conf_cache_name!r}")

        with gzip.open(self.conf_cache_path, mode='w') as file:
            file.write(json.dumps(
                {
                    "create": f"{datetime.datetime.now().strftime(date_format)}",
                    "config": self.get_config(
                        module=self.CONF_SUB_PATH, prefix=self.conf_file_prefix, suffix=self.conf_file_suffix)
                }, indent=4
            ).encode(encoding))

    def get_config_cache(
            self,
            conf_get_name: str,
            date_format: str = '%Y-%m-%d %H:%M:%S',
            decoding: str = 'utf-8'
    ) -> Dict[str, Any]:
        """
        Get configuration data from `json` file. If data does not exist,
        it will put config and repeat get again.
        """
        if self.conf_cache_path.exists():
            with gzip.open(self.conf_cache_path, mode='r') as file:
                conf_data = json.loads(file.read().decode(decoding))
                if (dt := conf_data.get("create")) and (
                        datetime.datetime.strptime(dt, date_format) + datetime.timedelta(**self.conf_cache_time)
                ) >= datetime.datetime.now():
                    return conf_data.get("config").get(conf_get_name, {})

        if self.verbose:
            logger.info("Does not found config `json` file or cache time was expired")

        # TODO: add version stamp on file name
        self.put_config_cache()
        return self.get_config_cache(conf_get_name)

    @property
    def validate_schemas(self) -> bool:
        """Validate config schemas with must-have keys"""
        return set(self.conf_schemas.keys()).issubset(set(self.conf_data.keys())) if self.conf_schemas else True

    @property
    def validate_class(self) -> bool:
        """Validate type of config match with class variable `CLASS_VALIDATE`"""
        if self.CLASS_VALIDATE:
            return any(self.conf_type.endswith(cls.__name__) for cls in self.CLASS_VALIDATE)
        return False

    @staticmethod
    def get_config(
            conf_name: Optional[str] = None,
            module: Optional[str] = None,
            prefix: str = '',
            suffix: str = '',
            encoding: str = 'utf-8'
    ) -> Dict[str, Any]:
        """Get config enhance function base on `parse_config`"""
        sub_path: str = f'{module}/' if module else ''
        conf: dict = {}
        for path_object in Path(ConfigParser.CONF_PATH).glob(f'{sub_path}{prefix}*{suffix}.yaml'):
            if path_object.is_file() and path_object.stat().st_size != 0:
                catalog_data: Dict[str, Any] = parse_config(str(path_object), encoding=encoding)
                if not conf_name:
                    conf: dict = merge_dicts(conf, catalog_data)
                elif conf_name in catalog_data:
                    return catalog_data[conf_name]
        return conf


class ConfigConvert(ConfigParser):
    """Convert class instance to new class in `CLASS_VALIDATE`"""

    def __init__(
            self,
            conf_name: str,
            conf_file_prefix: Optional[str] = None,
            conf_file_suffix: Optional[str] = None
    ):
        super(ConfigConvert, self).__init__(conf_name, conf_file_prefix, conf_file_suffix)
        if self.verbose:
            logger.info(f"Start mapping configuration with type: {self.conf_type}")

        _conf_name = self.conf_name
        _conf_data = self.conf_data
        if self.validate_sub_path:
            self.__class__ = import_string(self.conf_type)
            self.__init__(_conf_name, **_conf_data)

    # TODO: `__getstate__` and `__setstate__`
    # def __getattr__(self, attr):
    #     return getattr(self, attr)

    @property
    def validate_sub_path(self):
        """Validate class variable `CONF_SUB_PATH` value is exists in `CONF_PATH`"""
        return True


class ConfigMapping(ConfigParser):
    """Mapping class instance in `CLASS_VALIDATE` to property `model` and change itself to object"""
    __slots__ = 'model'

    def __init__(
            self,
            conf_name: str,
            conf_file_prefix: Optional[str] = None,
            conf_file_suffix: Optional[str] = None
    ):
        super(ConfigMapping, self).__init__(conf_name, conf_file_prefix, conf_file_suffix)
        if self.verbose:
            logger.info(f"Start mapping configuration with type: {self.conf_type}")

        _type_cls: Callable = import_string(self.conf_type)
        self.model: Any = _type_cls(self.conf_name, **self.conf_data)

    @property
    def validate_sub_path(self):
        """Validate class variable `CONF_SUB_PATH` value is exists in `CONF_PATH`"""
        return True


class ConfigDefaultMapping(ConfigMapping):
    CONF_SUB_PATH: ClassVar[str] = 'defaults'
    CLASS_VALIDATE: List[object] = [
        postgresql.PostgresTable,
        local.LocalCSVFile
    ]


class ConfigDefaultConvert(ConfigConvert):
    CONF_SUB_PATH: ClassVar[str] = 'defaults'
    CLASS_VALIDATE: List[object] = [
        postgresql.PostgresTable,
        local.LocalCSVFile
    ]
