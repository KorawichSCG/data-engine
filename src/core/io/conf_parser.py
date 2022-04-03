import mmap
import re
import os
import io
import logging
from typing import Optional
try:
    from yaml import safe_load
except ImportError:
    safe_load = None

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# (\\)?(\$)(\{?([A-Z0-9_]+)\}?)
# RE_DOTENV_VAR: re.Pattern = re.compile(r'(\\)?(\$)({?([A-Z0-9_]+)}?)', re.IGNORECASE)
RE_DOTENV_VAR: re.Pattern = re.compile(r"""
    (\\)?    # is it escaped with a backslash?
    (\$)                # literal $
    (                   # collect braces with var for sub
        \{?             #   allow brace wrapping
        ([A-Z0-9_]+)    #   match the variable
        \}?             #   closing brace
    )                   # braces end
""", re.IGNORECASE | re.VERBOSE)

# [\"\']?(\$(?:(?P<escaped>\$|\d+)|(\{(?P<braced>.*?)(\:(?P<braced_default>.*?))?\})))[\"\']?
# RE_YAML = re.compile(r'[\"\']?(\$(?:(?P<escaped>\$|\d+)|(\{(?P<braced>.*?)(\:(?P<braced_default>.*?))?\})))[\"\']?'
RE_YAML: re.Pattern = re.compile(r"""
    [\"\']?                                 # single or double quoted value
    (\$(?:                                  # start with non-capturing group
        (?P<escaped>\$|\d+)                 # escape $ or number like $1
        |
        (\{(?P<braced>.*?)                  # value if use braced {}
        (\:(?P<braced_default>.*?))?\})     # value default with sep :
    ))
    [\"\']?                                 # single or double quoted value
""", re.MULTILINE | re.UNICODE | re.IGNORECASE | re.VERBOSE)

# (\s|^)#.*
RE_YAML_COMMENT: re.Pattern = re.compile(r"(\s|^)#.*", re.MULTILINE | re.UNICODE | re.IGNORECASE)

# (?:^|^)\s*(?:export\s+)?(?P<name>[\w.-]+)(?:\s*=\s*?|:\s+?)(?P<value>\s*'(?:\\'|[^'])*'|\s*"(?:\\"|[^"])*"|\s*`(?:\\`|[^`])*`|[^#\r\n]+)?\s*(?:#.*)?(?:$|$)
RE_DOTENV: re.Pattern = re.compile(r"""
    ^\s*(?:export\s+)?      # optional export
    (?P<name>[\w.-]+)       # name of key
    (?:\s*=\s*?|:\s+?)      # separator `=` or `:`
    (?P<value>
        \s*\'(?:\\'|[^'])*\'    # single quoted value
        |
        \s*\"(?:\\"|[^"])*\"    # double quoted value
        |
        \s*`(?:\\`|[^`])*`      # backticks value
        |
        [^#\r\n]+           # unquoted value
    )?\s*                   # optional space
    $
""", re.MULTILINE | re.VERBOSE)

# {{\s*(?P<value>[\w\s\{\}$_]+)*\s*}}
# (?:\{\{)\s*(?P<value>[\w_\{\}\s]+)*\s*(?:\}\})
RE_PARAM: re.Pattern = re.compile(r"""
    {{\s*                   # start double brace {{
    (?P<value>
        [\w\s\{\}\$\@\_]+   # filter value
    )*
    \s*}}                   # end double brace }}
""", re.VERBOSE)


class ParseConfig:
    """
    Class of parse configuration that load from `.yaml` or parse environment variable `.env`
    files or contents of files (data) and resolve any environment variables. The environment
    variables must have format to be parsed: ${VAR_NAME} or $VAR_NAME
    """
    __version__ = '0.1'

    @classmethod
    def load(
            cls,
            path: str, /,
            parameter: Optional[dict] = None,
            **kwargs
    ):
        if 'override_flag' in kwargs:
            raise NotImplementedError("`load` does not support for argument `override_flag`")
        return cls(path=path, parameter=parameter, **kwargs)

    @classmethod
    def loads(
            cls,
            data: str, /,
            parameter: Optional[dict] = None,
            **kwargs
    ):
        if 'override_flag' in kwargs:
            raise NotImplementedError("`load` does not support for argument `override_flag`")
        return cls(data=data, parameter=parameter, **kwargs)

    @classmethod
    def load_env(
            cls,
            path: str, /,
            override: Optional[bool] = False
    ):
        return cls(path=path, override_flag=override, conf_type='env')

    @classmethod
    def loads_env(
            cls,
            data: str, /,
            override: Optional[bool] = False
    ):
        return cls(data=data, override_flag=override, conf_type='env')

    def __init__(
            self,
            path: Optional[str] = None,
            data: Optional[str] = None,
            parameter: Optional[dict] = None,
            default_sep: str = ':',
            default_value: str = 'N/A',
            raise_if_default_not_exists: bool = False,
            memory_read: bool = True,
            encoding: str = 'utf-8',
            loader: Optional[callable] = safe_load,
            override_flag: bool = False,
            conf_type: str = 'yaml'
    ):
        """
        Create `ParseConfig` class instance
        :param str path: the path to the yaml files
        :param str data: the yaml data itself as a stream
        :param dict parameter:
        :param str default_sep: if any default values are set, use this field to separate
            them from the environment variable name.
        :param str default_value: default value if the environment variable does not exists
        :param bool raise_if_default_not_exists: raise an exception if there is no default
            value set for the env variable.
        :param bool memory_read: flag of file reading by `mmap`
        :param str encoding: encoding
        :param yaml.loader loader: Specify which loader to use. Defaults to yaml.SafeLoader
        """
        if path is None and data is None:
            raise TypeError(f"{self.__class__.__name__} missing required argument: `path` or `data`")
        if safe_load is None:
            raise ModuleNotFoundError(
                f'{self.__class__.__name__} require "pyyaml >= 5". '
                'Please install this module into your environment'
            )
        # TODO: add validate path before parse to `self`. If not found, it will add `PROJ_PATH` before.
        self.__path: Optional[str] = path
        self.__data: Optional[str] = data
        self.__parameters: dict = parameter
        self.__default_sep: str = default_sep
        self.__default_value: str = default_value
        self.__memory_read: bool = memory_read
        self.__encoding: str = encoding
        self.__loader: callable = loader
        self.conf_junction(conf_type)
        if conf_type == 'env':
            self.__contents: str = self.__open_file(
                self.__path,
                self.__memory_read,
                self.__encoding
            ) if self.__path else self.__data
            self.__load_env(self.__prepare_env(self.__contents), override_flag)
        else:
            if self.__default_sep != ':':
                raise NotImplementedError(f"{self.__class__.__name__} does not support sep {self.__default_sep!r}")
            self.__contents: str = self.__open_file(
                self.__path,
                self.__memory_read,
                self.__encoding
            ) if self.__path else self.__data
            self.__raw_data: str = self.__prepare_yaml(
                self.__contents,
                self.__default_value,
                raise_if_default_not_exists
            )
            self.__dict = self.__read_yaml(self.__raw_data, self.__loader)
            for _data in self.__dict:
                super(ParseConfig, self).__setattr__(_data, self.__dict[_data])

    def __repr__(self):
        return f"{self.__class__.__name__}(type: {'data' if self.__data else 'file'}, keep: {len(self.__dict)})"

    def __iter__(self):
        return iter(self.__dict.keys())

    def __contains__(self, item) -> bool:
        """Check if key exists in `.yaml` configuration"""
        return item in self.__dict

    def __getitem__(self, item):
        """Get item when use `cls['item']`"""
        return self.__dict[item]

    def __setitem__(self, key, value):
        """Force to raise error when try to set item, like `cls['item'] = ?`"""
        raise NotImplementedError(f"{self.__class__.__name__} does not support for set item configuration")

    def __getattr__(self, attr):
        """
        Get attribute when use `cls.attr` that mean
        `cls.__dict__['attr'].__getattr__(instance, cls)`
        """
        return getattr(self.__dict, attr)

    def __setattr__(self, attr, value):
        """
        Force to raise error when try to set attribute, like `cls.attr = ?`
        that mean
        `cls.__dict__['attr'].__setattr__(instance, value)`
        """
        if not attr.startswith(f'_{self.__class__.__name__}__'):
            raise NotImplementedError(f"{self.__class__.__name__} does not support for set attribute configuration")
        super(ParseConfig, self).__setattr__(attr, value)

    def export(self):
        """Export configuration"""
        return self.__dict.copy()

    def conf_junction(self, _conf_type: str):
        """Junction of configuration type"""
        if os.path.isdir(self.__path):
            _file_name = '.env' if _conf_type == 'env' else 'config.yaml'
            self.__path = os.path.join(self.__path, _file_name)

    @staticmethod
    def __open_file(
            file_path: str,
            memory_read: bool,
            encoding: str
    ) -> str:
        """Read file from input path and filter out comment in `.yaml`"""
        with io.open(file_path, mode='r', encoding=encoding) as f:
            if memory_read:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                    return RE_YAML_COMMENT.sub("", mmap_obj.read().decode(encoding))
            return RE_YAML_COMMENT.sub("", f.read())

    @staticmethod
    def __prepare_yaml(
            contents: str,
            default_value: str,
            raise_if_default_not_exists: bool,
            escape_replaced: str = 'ESC'
    ) -> str:
        """Prepare content data from `.yaml` file before parse to `yaml.load`"""
        shifting: int = 0
        replaces: dict = {}
        replaces_esc: dict = {}
        for content in RE_YAML.finditer(contents):
            search: str = content.group(1)
            if not (escaped := content.group("escaped")):
                variable: str = content.group('braced')
                default: str = content.group('braced_default')
                if not default and raise_if_default_not_exists:
                    raise ValueError(
                        f'Could not find default value for {variable} in `.yaml` file'
                    )
                elif not variable:
                    raise ValueError(
                        f'Value {search!r} in `.yaml` file has something wrong with regular expression'
                    )
                replaces[search] = os.environ.get(variable, default) or default_value
            elif '$' in escaped:
                span = content.span()
                search = f'${{{escape_replaced}{escaped}}}'
                contents = (
                        contents[:span[0] + shifting] + search + contents[span[1] + shifting:]
                )
                shifting += len(search) - (span[1] - span[0])
                replaces_esc[search] = '$'
        for _replace in sorted(replaces, reverse=True):
            contents = contents.replace(_replace, replaces[_replace])
        for _replace in sorted(replaces_esc, reverse=True):
            contents = contents.replace(_replace, replaces_esc[_replace])
        return contents

    @staticmethod
    def __read_yaml(
            contents: str,
            loader: callable
    ):
        """Parse content data to `yaml.load`"""
        # TODO: change `__main__` dynamic to `__name__` with config logging
        logger.debug(f"Start load content in __name__ is {__name__} ...")
        return loader(contents)

    @staticmethod
    def __prepare_env(
            contents: str,
            keep_newline: bool = False,
            default_value: str = ''
    ) -> dict:
        """Prepare content data from `.env` file before load to os environment"""
        env: dict = {}
        for content in RE_DOTENV.finditer(contents):
            name: str = content.group('name')
            _value: str = content.group('value').strip()  # Remove leading/trailing whitespace
            if not _value:
                raise ValueError(
                    f'Value {name:!r} in `.env` file does not set value of variable'
                )
            value: str = _value if keep_newline else ''.join(_value.splitlines())
            quoted: Optional[str] = None

            # Remove surrounding quotes
            if m2 := re.match(r'^(?P<quoted>[\'\"`])(?P<value>.*)\1$', value):
                quoted: str = m2.group('quoted')
                value: str = m2.group('value')

            if quoted == "'":
                env[name] = value
                continue
            elif quoted == '"':
                # Unescape all chars except $ so variables can be escaped properly
                value: str = re.sub(r'\\([^$])', r'\1', value)

            # Substitute variables in a value
            for sub_content in RE_DOTENV_VAR.findall(value):
                replace: str = ''.join(sub_content[1:-1])
                if sub_content[0] != '\\':
                    # Replace it with the value from the environment
                    replace: str = env.get(sub_content[-1], os.environ.get(sub_content[-1], default_value))
                value: str = value.replace(''.join(sub_content[:-1]), replace)
            env[name] = value
        return env

    @staticmethod
    def __load_env(
            dotenv: dict,
            override_flag: bool
    ) -> None:
        """Parse environment file into `os.environ`"""
        for k, v in dotenv.items():
            if override_flag:
                os.environ[k] = v
            else:
                os.environ.setdefault(k, v)

    @staticmethod
    def __map_internal_params(
            contents: str,
            parameters: dict
    ):
        # TODO: create internal parameter parser
        """Parse parameters form input argument to value before return"""
        if not parameters:
            return contents
        for content in RE_PARAM.finditer(contents):
            if (value := content.group('value')) in parameters:
                replace = parameters[value]


conf: callable = ParseConfig

__all__ = ['conf']
