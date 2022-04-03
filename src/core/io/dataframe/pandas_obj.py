import os
import re
import itertools
from pathlib import Path
from typing import Any, Dict, Union, Optional
from src.core.utils import str_to_bool
from src.core.io.path_utils import path_join
from src.core.io.conf_parser import conf
from src.core.io.dataframe.plugins.pandas_plug import PandasCSVObject

os.environ.setdefault('PROJ_PATH', path_join(Path(__file__).parent, '../../../..'))
conf.load_env(path_join(os.environ['PROJ_PATH'], 'conf/.env'))
CONF_PATH = conf.load(
    f'{os.environ["PROJ_PATH"]}/conf/config.yaml'
)['datasets'][f'local.{os.environ["PROJ_ENV"]}']


class CSVColumn:
    """
    CSV column object
    config
    ------
        (i)   <column-name>:
                    datatype: <datatype>
                    unique (optional): False, [True, true, false]
                    nullable (optional): False, [True, true, false]
                    defaults (optional): <defaults>
                    check (optional): <check-statement>
                    comment (optional): <comment>

        (ii)  <column-name>: <datatype> [<unique>, <nullable>, <default>, ..., --<comment>]
    """
    COL_CONSTS: set = {'unique', 'not null', 'null', 'check', 'default', '--'}

    __slots__ = ('ps_col_datatype', 'ps_col_unique', 'ps_col_nullable', 'ps_col_check', 'ps_col_desc',
                 'ps_col_primary_key', 'ps_col_foreign_key', 'ps_col_default',)

    def __init__(
            self,
            ps_col: Union[str, Dict[str, Any]]
    ):
        self.ps_col_datatype: Optional[str] = None
        self.ps_col_nullable: Optional[str] = None
        self.ps_col_unique: Optional[str] = None
        self.ps_col_check: Optional[str] = None
        self.ps_col_default: Optional[str] = None
        self.ps_col_desc: Optional[str] = None
        if isinstance(ps_col, str):
            self.convert_from_string(ps_col)
        else:
            self.convert_from_mapping(ps_col)

    def convert_from_string(self, _ps_col: str):
        """
        Convert column statement form string to standard mapping
        example
        -------
            (i)    order_sales_value: "string"
        """
        _ps_detail_lower = _ps_col.lower()
        match_list: list = re.split(f"({'|'.join(list(self.COL_CONSTS))})", _ps_detail_lower.strip())
        match_datatype: str = match_list.pop(0).strip()
        match_dict: dict = dict(itertools.zip_longest(*[iter(match_list)] * 2, fillvalue=""))
        for k in self.COL_CONSTS:
            if k in match_dict:
                # Set True when column constraint in {`not null`, `null`, `unique`}
                match_dict[k] = True if (value := match_dict[k].strip()) == '' else value
            else:
                match_dict[k] = None if k in {'default', 'check', '--'} else False
        match_dict['nullable'] = not not_null if (not_null := match_dict.pop('not null')) else True
        match_dict['datatype'] = match_datatype
        match_dict['description'] = match_dict.pop('--')
        match_dict.pop('null')
        self.convert_from_mapping(match_dict)

    def convert_from_mapping(self, _ps_col: Dict[str, Any]):
        """
        Mapping schemas with dictionary
        example
        -------
        (i)   customer_id:
                    datatype: "string"
                    nullable: "false"
                    default: ""
        """
        self.ps_col_datatype: str = _ps_col.pop('datatype', 'text')
        self.ps_col_unique: bool = str_to_bool(_ps_col.pop('unique', False))
        self.ps_col_nullable: bool = str_to_bool(_ps_col.pop('nullable', False))
        self.ps_col_check: str = _ps_col.pop('check', None)
        self.ps_col_default: str = _ps_col.pop('default', None)
        self.ps_col_desc: Optional[str] = _ps_col.pop('description', None)

    def __str__(self):
        return f'{self.ps_col_datatype} {("null" if self.ps_col_nullable else "not null")}'

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ps_col_datatype})'

    @property
    def datatype(self):
        return self.ps_col_datatype

    @property
    def nullable(self):
        return self.ps_col_nullable

    @property
    def check(self):
        return self.ps_col_check

    @property
    def default(self):
        return self.ps_col_default

    @property
    def desc(self):
        return self.ps_col_desc

    @property
    def details(self):
        return {
            'datatype': self.ps_col_datatype,
            'nullable': self.ps_col_nullable,
            'check': self.ps_col_check,
            'default': self.ps_col_default,
            'description': self.ps_col_desc
        }


class PandasCSVFrame(PandasCSVObject):
    """
    Local csv files object
    config
    ------
        <files-alias-name>:
            type: io.datasets.PandasCSVFrame
            properties:
                catalog_name: <files-name>
                schemas:
                    (i)   <column-name>:
                                datatype: <datatype>
                                ...

                    (ii)  <column-name>: <datatype> []
                          ...

                header:
                encoding:
                ...
            retentions:
                ...
    example
    -------
        (i)   catalog_file_customer:
                    type: io.datasets.LocalCSVFile
                    properties:

    """
    CONF_PATH = CONF_PATH
    CONF_DELIMITER = '/'
    SUB_PATH = 'files'

    def __init__(
            self,
            catalog_name: str,
            external_params: dict,
            global_params: dict,
            properties: Dict[str, Any],
            **kwargs
    ):
        self.ps_server_conn = self.CONF_PATH['connection']
        self.ps_cat_name = properties.pop('catalog_name', catalog_name).split(self.CONF_DELIMITER)
        self.ps_file_name, self.ps_sub_path = self.ps_path_mapping(self.ps_cat_name)
        self.ps_file_type: str = properties.pop('catalog_type', self.OBJECT_TYPE)
        self.ps_ext_params: dict = external_params
        self.ps_cols: Optional[Dict[str, Any]] = properties.pop('schemas', None)

        # Properties for Pandas csv dataFrame
        # TODO: mapping `ps_cols` with `name` argument
        self.ps_file_names: Optional[list] = None
        self.ps_file_header: Optional[Union[int, list, str]] = properties.pop('header', 'infer')
        self.ps_file_usecols: callable = properties.pop('usecols', eval('lambda x: x'))
        self.ps_file_skipfooter: int = properties.pop('skipfooter', 0)
        self.ps_file_skip_blank_lines: bool = str_to_bool(properties.pop('skip_blank_lines', True))
        self.ps_file_encoding: str = properties.pop('encoding', 'utf-8')
        self.ps_file_delimiter: str = None if (not (_deli := properties.pop('delimiter', None)) or
                                               _deli in {r'\s+', r'\s'}) else _deli
        self.ps_file_delim_whitespace: bool = _deli in {r'\s+', r'\s'}
        self.ps_file_comment: str = properties.pop('comment', None)
        self.ps_file_thousands: str = properties.pop('thousands', ',')
        self.ps_file_engine: str = properties.pop('engine', 'python')

        # --- unset
        self.ps_file_converters: dict = properties.pop('converters', None)
        self.ps_file_on_bad_lines: str = properties.pop('on_bad_lines', 'warn')

        # Optional arguments for Pandas csv dataFrame
        self.ps_file_retentions: Optional[Dict[str, Any]] = kwargs.pop('retentions', {})

        super(PandasCSVFrame, self).__init__(
            self.ps_server_conn,
            self.ps_sub_path,
            self.ps_file_name,
            self.ps_file_type,
            self.ps_file_arguments,
            self.ps_ext_params
        )

    def __getattribute__(self, attr):
        if attr in super(PandasCSVFrame, self).__excluded__:
            raise AttributeError(f"{self.__class__} does not have attribute `{attr}`")
        return super(PandasCSVFrame, self).__getattribute__(attr)

    def ps_path_mapping(self, _split_path: list):
        """Generate sub path and file name from catalog configuration name"""
        # TODO: Check file extension be `.csv`, `.tsv`, or `.txt`
        _last_path: str = _split_path.pop(-1)
        _remain_path: str = '/'.join(_split_path)
        if '.' in _last_path:
            _file_path = _last_path
            _sub_path = f"/{_remain_path}" if _remain_path else ''
        else:
            _last_path = f"/{_last_path}" if _last_path else ""
            _file_path = '*.csv'
            _sub_path = f"/{_remain_path}{_last_path}" if _remain_path else _last_path
        return _file_path, f'{self.SUB_PATH}{_sub_path}'

    def validate_arguments(self, _arguments: Optional[dict]) -> bool:
        _arguments = _arguments or {}
        for _arg in _arguments:
            return False

    def _schemas(self):
        """Generate raw configuration from `schemas` property"""
        return {k: CSVColumn(v) for k, v in self.ps_cols.items()}

    @property
    def schemas(self):
        return self._schemas()

    @property
    def ps_file_arguments(self):
        return {
            'names': self.ps_file_names,
            'header': self.ps_file_header,
            'usecols': self.ps_file_usecols,
            'skipfooter': self.ps_file_skipfooter,
            'skip_blank_lines': self.ps_file_skip_blank_lines,
            'encoding': self.ps_file_encoding,
            'delimiter': self.ps_file_delimiter,
            'delim_whitespace': self.ps_file_delim_whitespace,
            'comment': self.ps_file_comment,
            'thousands': self.ps_file_thousands,
            'engine': self.ps_file_engine
        }

    @staticmethod
    def get_str_or_list(props, key) -> list:
        return _return_key if isinstance((_return_key := props.pop(key, [])), list) else [_return_key]


class PandasExcelFrame(PandasCSVObject):
    """
        Local excel files object
        config
        ------
            <files-alias-name>:
                type: io.datasets.PandasExcelFrame
                properties:
                    catalog_name: <files-name>
                    schemas:
                        (i)   <column-name>:
                                    datatype: <datatype>
                                    ...

                        (ii)  <column-name>: <datatype> []
                              ...

                    header:
                    encoding:
                    ...
                retentions:
                    ...
        example
        -------
            (i)   catalog_file_customer:
                        type: io.datasets.LocalCSVFile
                        properties:

        """
    CONF_PATH = CONF_PATH
    CONF_DELIMITER = '/'
    SUB_PATH = 'files'

    def __init__(
            self,
            catalog_name: str,
            external_params: dict,
            global_params: dict,
            properties: Dict[str, Any],
            **kwargs
    ):
        self.ps_server_conn = self.CONF_PATH['connection']
        self.ps_cat_name = properties.pop('catalog_name', catalog_name).split(self.CONF_DELIMITER)
        self.ps_file_name, self.ps_sub_path = self.ps_path_mapping(self.ps_cat_name)
        self.ps_file_type: str = properties.pop('catalog_type', self.OBJECT_TYPE)
        self.ps_ext_params: dict = external_params
        self.ps_cols: Optional[Dict[str, Any]] = properties.pop('schemas', None)

        # Properties for Pandas csv dataFrame
        # TODO: mapping `ps_cols` with `name` argument
        self.ps_file_names: Optional[list] = None
        self.ps_file_header: Optional[Union[int, list, str]] = properties.pop('header', 'infer')
        self.ps_file_usecols: callable = properties.pop('usecols', eval('lambda x: x'))
        self.ps_file_skipfooter: int = properties.pop('skipfooter', 0)
        self.ps_file_skip_blank_lines: bool = str_to_bool(properties.pop('skip_blank_lines', True))
        self.ps_file_encoding: str = properties.pop('encoding', 'utf-8')
        self.ps_file_delimiter: str = None if (not (_deli := properties.pop('delimiter', None)) or
                                               _deli in {r'\s+', r'\s'}) else _deli
        self.ps_file_delim_whitespace: bool = _deli in {r'\s+', r'\s'}
        self.ps_file_comment: str = properties.pop('comment', None)
        self.ps_file_thousands: str = properties.pop('thousands', ',')
        self.ps_file_engine: str = properties.pop('engine', 'python')

        # --- unset
        self.ps_file_converters: dict = properties.pop('converters', None)
        self.ps_file_on_bad_lines: str = properties.pop('on_bad_lines', 'warn')

        # Optional arguments for Pandas csv dataFrame
        self.ps_file_retentions: Optional[Dict[str, Any]] = kwargs.pop('retentions', {})

        super(PandasExcelFrame, self).__init__(
            self.ps_server_conn,
            self.ps_sub_path,
            self.ps_file_name,
            self.ps_file_type,
            self.ps_file_arguments,
            self.ps_ext_params
        )

    @property
    def ps_file_arguments(self):
        return {
            'names': self.ps_file_names,
            'header': self.ps_file_header,
            'usecols': self.ps_file_usecols,
            'skipfooter': self.ps_file_skipfooter,
            'skip_blank_lines': self.ps_file_skip_blank_lines,
            'encoding': self.ps_file_encoding,
            'delimiter': self.ps_file_delimiter,
            'delim_whitespace': self.ps_file_delim_whitespace,
            'comment': self.ps_file_comment,
            'thousands': self.ps_file_thousands,
            'engine': self.ps_file_engine
        }
