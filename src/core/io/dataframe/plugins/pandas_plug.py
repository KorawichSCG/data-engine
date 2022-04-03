import os
from typing import Optional

import pandas as pd
from src.core.io.dataframe.plugins.file_plug import FileObject


class DataFrameObject:
    """
    Pandas DataFrame Object for read data with difference type
    """

    def __init__(
            self,
            server_conn,
            sub_path,
            file_name,
            file_type,
            file_arguments,
            ext_params
    ):
        self.server_conn = server_conn
        self.sub_path = sub_path
        self.file_name = file_name
        self.file_type = file_type

        # Set necessary argument for `csv` type
        self.delimiter = ','
        self.encoding = 'utf-8'
        self.header = True
        self.mangle_dupe_cols = True
        self.skipinitialspace = False
        self.skiprows = 0
        self.memory_map = True

        # Set necessary argument for `excel` type

        # Set necessary argument for `json` type

        # Set necessary argument for `html` type

        # Set necessary argument for `xml` type

        # Set necessary argument for `pickle` type

        # Set necessary argument for `parquet` type

    @property
    def full_path(self):
        """
        Local: files://localhost/path/to/table.parquet
        S3: s3://bucket/partition_dir
        """
        return os.path.join(self.server_conn, self.sub_path, f"{self.file_name}.{self.file_type}")

    @property
    def convert_pandas_type(self):
        if self.file_type == 'csv':
            return pd.read_csv

    def read(self) -> pd.DataFrame:
        return self.convert_pandas_type(self.full_path)

    def save(self):
        return None


class PandasColumnObject:
    """Pandas Column Object"""
    MAP_DATA_TYPES = {
        'object': '',
        'int64': '',
        'float64': '',
        'datetime64': '',
        'bool': ''
    }


class PandasCSVObject:
    """Pandas DataFrame for parse `csv` file"""
    OBJECT_TYPE: str = "csv"
    OBJECT_ARGS: set = {
        'names',
        'header',
        'usecols',
        'skipfooter',
        'encoding',
        'delimiter',
        'comment',
        'thousands',
        'engine'
    }
    __excluded__ = {'save'}

    def __init__(
            self,
            server_conn: str,
            sub_path: str,
            file_name: str,
            file_type: str = 'csv',
            file_arguments: dict = None,
            ext_params: dict = None
    ):
        self.server_conn: str = server_conn
        self.sub_path: str = sub_path
        self.file_name: str = file_name
        self.file_type: str = file_type
        self.file_arguments: dict = file_arguments
        if self.file_type not in {'txt', 'tsv', self.OBJECT_TYPE}:
            raise KeyError(f'file_type is {self.file_type} does not match with class')
        elif self.validate_arguments(file_arguments):
            raise KeyError(f'file_arguments does support for {file_arguments}')
        self.fs: FileObject = FileObject(
            root_path=server_conn,
            sub_path=sub_path,
            file_name=file_name,
            file_type=file_type,
            parameters=ext_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    @property
    def columns(self):
        return {}

    @property
    def df(self) -> pd.DataFrame:
        if self.fs.file_found == 1:
            for file in self.fs.open_files():
                return pd.read_csv(file, **self.file_arguments)
        elif self.fs.file_found > 1:
            _dfs = [pd.read_csv(file, **self.file_arguments) for file in self.fs.open_files()]
            return pd.concat(_dfs)
        else:
            return pd.DataFrame()


class PandasExcelObject:
    """Pandas DataFrame for parse `excel` file"""
    OBJECT_TYPE: str = "xlsx"
    OBJECT_ARGS: set = {
        'names',
        'header',
        'usecols',
        'skipfooter',
        'encoding',
        'delimiter',
        'comment',
        'thousands',
        'engine'
    }
    __excluded__ = {'save'}

    def __init__(
            self,
            server_conn: str,
            sub_path: str,
            file_name: str,
            file_type: str = 'excel',
            file_arguments: dict = None,
            ext_params: dict = None
    ):
        self.server_conn: str = server_conn
        self.sub_path: str = sub_path
        self.file_name: str = file_name
        self.file_type: str = file_type
        self.file_arguments: dict = file_arguments
        if self.file_type not in {'xlsx', 'xls', self.OBJECT_TYPE}:
            raise KeyError(f'file_type is {self.file_type} does not match with class')
        elif self.validate_arguments(file_arguments):
            raise KeyError(f'file_arguments does support for {file_arguments}')
        self.fs: FileObject = FileObject(
            root_path=server_conn,
            sub_path=sub_path,
            file_name=file_name,
            file_type=file_type,
            parameters=ext_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    @property
    def columns(self):
        return {}

    @property
    def df(self) -> pd.DataFrame:
        if self.fs.file_found == 1:
            for file in self.fs.open_files():
                return pd.read_excel(file, **self.file_arguments)
        elif self.fs.file_found > 1:
            _dfs = [pd.read_excel(file, **self.file_arguments) for file in self.fs.open_files()]
            return pd.concat(_dfs)
        else:
            return pd.DataFrame()
