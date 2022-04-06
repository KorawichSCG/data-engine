import io
import os
import numpy as np
import pandas as pd
from typing import Optional, Dict, Union, Protocol
from src.core.io.dataframe.plugins.file_plug import FileObject


class PandasFileObject:
    """Pandas DataFrame for parse any file format"""
    __version__ = '14.0.0'
    __excluded__ = {'save'}

    OBJECT_FUNC: Optional[callable] = None

    def __init__(
            self,
            file_conn: str,
            file_path: str,
            file_name: str,
            file_extension: str = 'csv',
            file_arguments: dict = None,
            ext_params: dict = None
    ):
        self.file_conn: str = file_conn
        self.file_path: str = file_path
        self.file_name: str = file_name
        self.file_extension: str = file_extension
        self.file_arguments: dict = self.file_args_logic(file_arguments)
        # TODO: edit logic of get default of arguments
        self.fs: FileObject = FileObject(
            root_path=file_conn,
            sub_path=file_path,
            file_name=file_name,
            file_extension=file_extension,
            parameters=ext_params
        )
        self.file_columns: Dict[str, PandasColumnObject] = self.generate_columns()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    @property
    def file_founds(self):
        return list(self.fs.get_files)

    def generate_columns(self):
        _columns: dict = dict(self._columns().items())
        return {
            col: PandasColumnObject(column_name=col, **v) for col, v in _columns.items()
        }

    def _columns(self) -> dict:
        """
        {
            <column-name>: {
                data_type: numpy.dtype : <object-data-type>,
                nullable: bool : [True, False]
            }
        }
        """
        _rows_num: int = self.rows_num
        _info_df: pd.DataFrame = pd.concat(
            [pd.DataFrame(data=self.df.dtypes, columns=['data_type']), self.df.count().rename('nullable')],
            axis=1
        )
        _info_df['nullable'] = np.where(_info_df['nullable'] < _rows_num, False, True)
        return _info_df.to_dict(orient='index')

    @property
    def rows_num(self):
        return len(self.df.index)

    @property
    def columns(self):
        return self.file_columns

    @property
    def df(self) -> pd.DataFrame:
        """DataFrame"""
        if self.fs.file_found == 1:
            for file in self.fs.open_files():
                return getattr(pd, self.OBJECT_FUNC)(file, **self.file_arguments)
        elif self.fs.file_found > 1:
            _dfs = [getattr(pd, self.OBJECT_FUNC)(file, **self.file_arguments) for file in self.fs.open_files()]
            return pd.concat(_dfs)
        return pd.DataFrame()

    @staticmethod
    def file_args_logic(_args: dict) -> dict: ...


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
            column_name: str,
            data_type: np.dtype,
            nullable: bool

    ):
        self.col_name: str = column_name
        self.col_datatype: np.dtype = data_type
        self.col_nullable: bool = nullable
        # print(type(data_type))

    def __repr__(self):
        return f"{self.col_datatype}{(' not null' if self.col_nullable else ' null')}"


class PandasCSVObject(PandasFileObject):
    """Pandas DataFrame for parse `csv` file"""
    OBJECT_FUNC = 'read_csv'

    @staticmethod
    def file_args_logic(_args: dict) -> dict:
        """
        Mapping logic convertor for Pandas Read CSV function
        """
        engine_mapping = {
            # The C and pyarrow engines are faster than python
            'python': {},  # More feature-complete
            'pyarrow': {},  # Multithreading is currently only supported
            'c': {}
        }

        # Main argument
        _sep: Optional[str] = _args.get('sep', None)
        _delimiter = _args.get('delimiter')

        # Header order by priority
        _header: Optional[Union[int, list, str]] = _args.get('header', 'infer')
        _names: Optional[list] = _args.get('names')
        _index_col: Optional[Union[int, str, list, bool]] = _args.get('index_col')
        _usecols = _args.get('usecols')
        _mangle_dupe_cols = _args.get('mangle_dupe_cols', True)

        _dtype = _args.get('dtype')
        _converters = _args.get('converters')

        _true_values = _args.get('true_values')
        _false_values = _args.get('false_values')

        _skipinitialspace = _args.get('skipinitialspace', False)
        _skiprows = _args.get('skiprows')  # callable
        _skipfooter = _args.get('skipfooter', 0)
        _nrows = _args.get('nrows')

        # NA value
        _na_values = _args.get('na_values')
        _keep_default_na = _args.get('keep_default_na', True)
        _na_filter = _args.get('na_filter', True)
        _verbose = _args.get('verbose', False)
        _skip_blank_lines = _args.get('skip_blank_lines', True)

        _parse_dates = _args.get('parse_dates', False)
        _infer_datetime_format = _args.get('infer_datetime_format', False)
        _keep_date_col = _args.get('keep_date_col', False)
        _date_parser = _args.get('date_parser')
        _dayfirst = _args.get('dayfirst', False)
        _cache_dates = _args.get('cache_dates', True)

        _thousands = _args.get('thousands')
        _decimal = _args.get('decimal', '.')

        # Quote and encoding
        _quotechar = _args.get('quotechar')
        _quoting = _args.get('quoting', 0)
        _escapechar = _args.get('escapechar')
        _comment = _args.get('comment')
        _encoding = _args.get('encoding', 'utf-8')

        _on_bad_lines = _args.get('on_bad_lines', 'error')

        _low_memory = _args.get('low_memory', True)
        _memory_map = _args.get('memory_map', False)
        _engine: str = _args.get('engine', 'python').lower()

        # C Engine should to set sep be ','
        if _engine == 'c' and not _sep:
            _sep: str = ','

        _sep_or_delimiter = {'delimiter': _delimiter} if _delimiter else {'sep': _sep}

        return {
            **_sep_or_delimiter,
            'header': _header,
            'names': _names,
            'index_col': _index_col,
            'usecols': _usecols,
            'mangle_dupe_cols': _mangle_dupe_cols,
            'engine': _engine,
            'on_bad_lines': _on_bad_lines
        }


class PandasExcelObject(PandasFileObject):
    """Pandas DataFrame for parse `excel` file"""
    OBJECT_FUNC = 'read_excel'

    @staticmethod
    def file_args_logic(_args: dict) -> dict:
        """
        Mapping logic convertor for Pandas Read EXCEL function
        """
        engine_mapping = {
            'xlrd': {},  # Support for .xls
            'openpyxl': {},  # Support latest file version
            'odf': {},  # Support document file formats (.odf, .ods, .odt)
            'pyxlsb': {}  # Supports binary excel files
        }

        # Main argument
        _sheet_name = _args.get('sheet_name', 0)

        # Header order by priority
        _header: Optional[Union[int, list, str]] = _args.get('header', 'infer')
        _names: Optional[list] = _args.get('names')
        _index_col: Optional[Union[int, str, list, bool]] = _args.get('index_col')
        _usecols = _args.get('usecols')
        _mangle_dupe_cols = _args.get('mangle_dupe_cols', True)

        _dtype = _args.get('dtype')
        _converters = _args.get('converters')

        _true_values = _args.get('true_values')
        _false_values = _args.get('false_values')

        _skiprows = _args.get('skiprows')  # callable
        _skipfooter = _args.get('skipfooter', 0)
        _nrows = _args.get('nrows')

        # NA value
        _na_values = _args.get('na_values')
        _keep_default_na = _args.get('keep_default_na', True)
        _na_filter = _args.get('na_filter', True)
        _verbose = _args.get('verbose', False)

        _parse_dates = _args.get('parse_dates', False)
        _date_parser = _args.get('date_parser')

        _thousands = _args.get('thousands')
        _decimal = _args.get('decimal', '.')

        _comment = _args.get('comment')

        _engine: str = _args.get('engine', 'openpyxl').lower()

        return {
            'sheet_name': _sheet_name,
            'header': _header,
            'names': _names,
            'index_col': _index_col,
            'usecols': _usecols,
            'mangle_dupe_cols': _mangle_dupe_cols,
            'skiprows': _skiprows,
            'comment': _comment,
            'engine': _engine,
        }


class PandasJsonObject(PandasFileObject):
    """Pandas DataFrame for parse `json` file"""
    OBJECT_FUNC = 'read_json'

    @staticmethod
    def file_args_logic(_args: dict) -> dict:
        """
        Mapping logic convertor for Pandas Read JSON function

        Indication of expected JSON string format. Compatible JSON strings can be produced
        by to_json() with a corresponding orient value. The set of possible orients is:
            - 'split' : dict like {index -> [index], columns -> [columns], data -> [values]}
            - 'records' : list like [{column -> value}, ... , {column -> value}]
            - 'index' : dict like {index -> {column -> value}}
            - 'columns' : dict like {column -> {index -> value}}
            - 'values' : just the values array

        The allowed and default values depend on the value of the typ parameter.
        - when typ == 'series',
            - allowed orients are {'split','records','index'}
            - default is 'index'
            - The Series index must be unique for orient 'index'.
        - when typ == 'frame',
            - allowed orients are {'split','records','index', 'columns','values', 'table'}
            - default is 'columns'
            - The DataFrame index must be unique for orients 'index' and 'columns'.
            - The DataFrame columns must be unique for orients 'index', 'columns', and 'records'.
        """

        # Main argument
        _typ = _args.get('typ', 'frame')
        _orient = _args.get('orient', ('index' if _typ == 'series' else 'columns'))

        _dtype = _args.get('dtype')
        _convert_axes = _args.get('convert_axes')
        _convert_dates = _args.get('convert_dates', True)
        _keep_default_dates = _args.get('keep_default_dates', True)

        _precise_float = _args.get('precise_float', False)
        _date_unit = _args.get('date_unit')
        _encoding = _args.get('encoding', 'utf-8')

        _lines = _args.get('lines')
        _nrows = _args.get('nrows')

        if _orient == 'table':
            _dtype = None
            _convert_axes = None

        return {
            'typ': _typ,
            'orient': _orient,
            'encoding': _encoding
        }
