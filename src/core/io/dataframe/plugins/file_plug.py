import datetime
import re
import pandas as pd
import fsspec
import os
import logging
import pytz
from typing import Generator, Optional
from src.core.io.path_utils import path_join
from src.core.utils import merge_dicts

PROJ_PATH = os.environ.get('PROJ_PATH', path_join(os.path.dirname(__file__), '../../../../..'))
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class FileObject:
    """
    File object for validate input path with regular expression
    config
    ------
        (i)     root_path: 'file:///C:/username/root_path/sub_path/file_*.csv'
                sub_path: 'sub_path/*'
                file_name: 'file_*.csv'
    """
    MAP_DATE_PARAM: dict = {
        # If `run_date` is '2022-03-01 15:30:59.599', the mapping will be
        'year': '%Y',  # 2022
        'year-short': '%y',  # 22
        # 'month-exc': '%-m',  # 3
        'month': '%m',  # 03
        'month-exc': ('%#m' if os.name == 'nt' else '%-m'),  # 3 NOTE: `#` use in window but linux is `-`
        'month-short': '%B',  # Mar
        'month-long': '%b',  # March
        # 'day-exc': '%e',  # 1
        'day': '%d',  # 01
        'date': '%a',  # Tue
        'date-long': '%A',  # Tuesday
        'hour': '%H',  # 15
        'hour-12': '%I',  # 03
        'minute': '%M',  # 15
        'second': '%S',  # 15
    }
    MODE_BYTE: set = {'xlsx', 'xls'}
    FILE_FILTER: set = {'~', '$', '@', '!', '^'}

    def __init__(
            self,
            root_path: str,
            sub_path: str,
            file_name: str,
            file_extension: str,
            parameters: Optional[dict] = None,
            regex: bool = True,
            open_mode: str = 'r',
            encoding: str = 'utf-8',
            path_sep: str = '/'
    ):
        """
        Create file object instance and convert regular expression with parameters
        Example
        -------
            (i)     root_path: C://user/path/
                    sub_path: csv/customer/{year}/{month}
                    file_name: customer_{year}{month}{day}.csv

            (ii)    full_path: C://user/path/csv/billing_{product_group_id}_{year}{month}{day}.csv
        """
        self.root_path: str = root_path.removesuffix(path_sep)
        self.sub_path: str = sub_path.removesuffix(path_sep)
        self.parameters: dict = self.__generate_params(parameters)
        self.file_name: str = file_name.replace('*', r'([\w\d_]*)') if regex else file_name
        self.files: list = []
        self.open_mode = f'{open_mode}b' if file_extension in self.MODE_BYTE else open_mode
        self.encoding = encoding
        try:
            # TODO: change way to search file that mean use regex before use format
            self.file_name_search = f'{self.sub_path}/{self.file_name}'.format(**self.parameters)
            self.file_sys_mapper = fsspec.get_mapper(self.root_path)
            for _ in self.file_sys_mapper:
                if (
                        re.search(self.file_name_search, _) and
                        all(val not in _ for val in self.FILE_FILTER)
                ):
                    logger.debug(f'Found a file: {_}')
                    self.files.append(f'{self.root_path}{path_sep}{_}')
            self._connect = True
        except KeyError as err:
            logger.warning(f'Parameters for `sub_path` or `file_name` does not add for key: {err}')
        except ValueError as err:
            logger.warning(f'{err}, please check root path: {self.root_path}')
            self._connect = False
        else:
            if not self.files:
                logger.warning(f'Files does not found with regex: {self.file_name_search}')

    def __generate_params(
            self,
            _params: Optional[dict] = None,
            _date_fmt: str = '%Y-%m-%d %H:%M:%S',
            _date_timezone: str = 'Asia/Bangkok',
            _p_run_date: str = 'run_date',
            _p_timestamp: str = 'timestamp'
    ):
        """Mapping input with needed parameters like `run_date`, `timestamp`"""
        _params = _params or {}
        if any(need in _params for need in {_p_timestamp, _p_run_date}):
            if _p_run_date not in _params:
                _params = merge_dicts(
                    _params,
                    {_p_run_date: datetime.datetime.fromtimestamp(_params.get(_p_timestamp)).strftime(_date_fmt)}
                )
            elif _p_timestamp not in _params:
                _params = merge_dicts(
                    _params,
                    {_p_timestamp: round(datetime.datetime.strptime(_params.get(_p_run_date), _date_fmt).timestamp())}
                )
        else:
            _params = merge_dicts(
                _params, {
                    _p_run_date: datetime.datetime.now(pytz.timezone(_date_timezone)).strftime(_date_fmt),
                    _p_timestamp: round(datetime.datetime.now(pytz.timezone(_date_timezone)).timestamp())
                }
            )
        return merge_dicts({
            _p: datetime.datetime.fromtimestamp(_params.get(_p_timestamp)).strftime(_fmt)
            for _p, _fmt in self.MAP_DATE_PARAM.items()
        }, _params)

    @property
    def file_found(self):
        return len(self.files)

    @property
    def get_files(self) -> Generator:
        yield from self.files

    def open_files(self) -> Generator:
        for _ in self.get_files:
            with fsspec.open(_, mode=self.open_mode, encoding=self.encoding) as file:
                yield file

    @property
    def connectable(self) -> bool:
        return self._connect
