import io
import os

import numpy as np
import pandas as pd
from typing import Optional
from src.core.io.dataframe.plugins.file_plug import FileObject


class PandasColumnObject:
    """Pandas Column Object"""
    MAP_DATA_TYPES = {
        'object': '',
        'int64': '',
        'float64': '',
        'datetime64': '',
        'bool': ''
    }

    def __init__(
            self,
            column_name,
            data_type,
            nullable

    ):
        self.col_name = column_name


class PandasCSVObject:
    """Pandas DataFrame for parse `csv` file"""
    OBJECT_TYPE: set = {'csv', 'txt', 'tsv'}
    OBJECT_ARGS: set = {
        'names',
        'header',
        'delimiter',
        'index_col',
        'dtype',
        'converters',
        'usecols',
        'true_values',
        'false_values',
        'skipinitialspace',
        'skiprows',
        'skipfooter',
        'nrows',
        'na_values',
        'keep_default_na',
        'skip_blank_lines',
        'encoding',
        'mangle_dupe_cols',
        'comment',
        'thousands',
        'decimal',
        'quotechar',
        'quoting',
        'escapechar',
        'on_bad_lines',
        'low_memory',
        'memory_map',
        'engine'
    }
    __excluded__ = {'save'}

    def __init__(
            self,
            file_conn: str,
            file_path: str,
            file_name: str,
            file_type: str = 'csv',
            file_arguments: dict = None,
            ext_params: dict = None
    ):
        self.file_conn: str = file_conn
        self.file_path: str = file_path
        self.file_name: str = file_name
        self.file_type: str = file_type
        self.file_arguments: dict = self.__args_logic_mapping(file_arguments)
        if self.file_type not in self.OBJECT_TYPE:
            raise KeyError(f'file_type is {self.file_type} does not match with class')
        self.fs: FileObject = FileObject(
            root_path=file_conn,
            sub_path=file_path,
            file_name=file_name,
            file_type=file_type,
            parameters=ext_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    @property
    def file_full_name(self) -> str:
        return '/'.join([self.file_conn, self.file_path, self.file_name])

    @property
    def file_founds(self):
        return list(self.fs.get_files)

    def _columns(self) -> dict:
        _rows_num: int = self.rows_num
        _info_df: pd.DataFrame = pd.concat(
            [pd.DataFrame(data=self.df.dtypes, columns=['data_type']), self.df.count().rename('nullable')],
            axis=1
        )
        _info_df['nullable'] = np.where(_info_df['nullable'] < _rows_num, True, False)
        return _info_df.to_dict(orient='index')

    def generate_columns(self):
        _columns: dict = dict(self._columns().items())
        return {
            col: PandasColumnObject({'column_name': col, **v}) for col, v in _columns.items()
        }

    @property
    def rows_num(self):
        return len(self.df.index)

    @property
    def df(self) -> pd.DataFrame:
        """DataFrame"""
        if self.fs.file_found == 1:
            for file in self.fs.open_files():
                return pd.read_csv(file, **self.file_arguments)
        elif self.fs.file_found > 1:
            _dfs = [pd.read_csv(file, **self.file_arguments) for file in self.fs.open_files()]
            return pd.concat(_dfs)
        else:
            return pd.DataFrame()

    def __args_logic_mapping(self, _arguments: dict):
        """
        Mapping pandas read_csv arguments logic with correct values
        """
        return {
            'names': None,
            'header': _arguments.get('header', 'infer'),
            'usecols': eval(_arguments.get('usecols', 'lambda x: x')),
            'skipfooter': _arguments.get('skipfooter', 0),
            'skip_blank_lines': _arguments.get('skip_blank_lines', True),
            'encoding': _arguments.get('encoding', 'utf-8'),
            'delimiter': None if (not (_deli := _arguments.get('delimiter', None)) or
                                  _deli in {r'\s+', r'\s'}) else _deli,
            'delim_whitespace': _deli in {r'\s+', r'\s'},
            'comment': _arguments.get('comment', None),
            'thousands': _arguments.get('thousands', ','),
            'engine': _arguments.get('engine', 'python'),
            'converters': _arguments.get('converters', None),
            'on_bad_lines': _arguments.get('on_bad_lines', 'warn'),
        }
