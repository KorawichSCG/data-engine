import re
from typing import Generator
import pandas as pd
import fsspec
import os
from src.core.io.path_utils import path_join
from pathlib import Path

PROJ_PATH = os.environ.get('PROJ_PATH', path_join(os.path.dirname(__file__), '../../..'))


class FileObject:
    """
    File object for validate input path with regular expression
    config
    ------
        (i)     root_path: 'file:///C:/username/root_path/sub_path/file_*.csv'
                sub_path: 'sub_path/*'
                file_name: 'file_*.csv'
    """

    FORMAT_MAP: dict = {
        '@{year}': '%Y',  # 2022
        '@{month}': '%m',  # 03
        '@{month~}': '%B',  # Mar
        '@{month!}': '%b',  # March
        '@{day!}': '%-d',  # 1
        '@{day}': '%d',  # 01
    }

    def __init__(
            self,
            root_path: str,
            sub_path: str,
            file_name: str,
            regex: bool = True
    ):
        """
        Create file object instance
        Example
        -------
            (i)     root_path: <pure-root-path contain 'C://user/path/'>
                    sub_path: <regex-sub-path>
                        -   csv/customer/YYYY/MM/
                        -   csv/customer/@{year}/@{month}
                    file_name: <regex-file-name>
                        -   customer_@{year}@{month}@{day}.csv
        """
        self.root_path: str = root_path.removesuffix('/')
        self.sub_path: str = sub_path.removesuffix('/')
        self.file_name: str = file_name.replace('*', r'([\w\d]*)')
        self.files: list = []
        if regex:
            try:
                self.file_sys_mapper = fsspec.get_mapper(self.root_path)
                for _ in self.file_sys_mapper:
                    if re.search(self.sub_path, _) and re.search(self.file_name, _):
                        self.files.append(f'{self.root_path}/{_}')
                self._connect = True
            except ValueError:
                self._connect = False

    @property
    def get_files(self) -> Generator:
        yield from self.files

    def open_files(self) -> Generator:
        for _ in self.get_files:
            yield fsspec.open(_)

    @property
    def connectable(self) -> bool:
        return self._connect


if __name__ == '__main__':
    fs = FileObject(f'file:///{PROJ_PATH}/data/sandbox/', 'files/csv', 'billing_*.csv')
    for _ in fs.open_files():
        df = pd.read_csv(_)
        print(df)
