# Reference: https://github.com/mkaranasou/pyaml_env
# Reference: https://github.com/theskumar/python-dotenv
# Reference: https://github.com/jpadilla/django-dotenv
import yaml
import mmap
import os
import re
import sys
import warnings
from typing import Dict, Optional, Any


class BaseConfig:
    """
    Example
    -------
        path_data = 'local_file: path: "data/files/files.csv"'
        conf.test = BaseConfig(parse_config(data=path_data, tag='!env'))
        config.local_file.path
    """
    def __init__(self, config_dict):
        if config_dict:
            self.__dict__.update(**{
                k: v for k, v in self.__class__.__dict__.items()
                if '__' not in k and not callable(v)
            })
            self.__dict__.update(**config_dict)
        self._is_validated = False
        self._is_valid = False
        self._errors = []
        self.__dict__ = self.__handle_inner_structures()

    def __handle_inner_structures(self):
        for k, v in self.__dict__.items():
            if isinstance(v, dict):
                self.__dict__[k] = BaseConfig(v)
        return self.__dict__

    @property
    def errors(self):
        return self._errors

    def validate(self):
        raise NotImplementedError()


def parse_config(
        path=None, data=None, tag: str = '!ENV', default_sep: str = ':',
        default_value: str = 'N/A', raise_if_na: bool = False,
        memory_read: bool = True, encoding: str = 'utf-8',
        loader: yaml.loader = yaml.SafeLoader
) -> Dict[str, Any]:
    """
    Load yaml configuration from path or from the contents of a files (data)
    and resolve any environment variables. The environment variables
    must have the tag e.g. !ENV *before* them and be in this format to be
    parsed: ${VAR_NAME}

    Example
    -------
        database:
          name: test_db
          username: !ENV ${DB_USER:paws}
          password: !ENV ${DB_PASS:meaw}
          url: !ENV 'http://${DB_BASE_URL:straight_to_production}:${DB_PORT:12345}'

    :param str encoding: encoding
    :param str path: the path to the yaml files
    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for, if None, all env variables will be
        resolved.
    :param str default_sep: if any default values are set, use this field
        to separate them from the enironment variable name. E.g. ':' can be
        used.
    :param str default_value: the tag to look for
    :param bool raise_if_na: raise an exception if there is no default
        value set for the env variable.
    :param bool memory_read: the flag of read files by `mmap`
    :param Type[yaml.loader] loader: Specify which loader to use. Defaults to
        yaml.SafeLoader
    :return: the dict configuration
    :rtype: Dict[str, Any]
    """
    default_sep = default_sep or ''
    default_sep_pattern = r'(' + default_sep + r'[^}]+)?' if default_sep else ''
    pattern = re.compile(
        r'.*?\$\{([^}{' + default_sep + r']+)' + default_sep_pattern + r'\}.*?')
    loader = loader or yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    # e.g. sample_key: !ENV "some_string${ENV_VAR}other_stuff_follows"
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(_loader: yaml.loader, node):
        """
        Extracts the environment variable from the yaml node's value
        :param yaml.Loader _loader: the yaml loader (as defined above)
        :param node: the current node (key-value) in the yaml
        :return: the parsed string that contains the value of the environment
            variable or the default value if defined for the variable. If no value
            for the variable can be found, then the value is replaced by
            default_value='N/A'
        """
        value = _loader.construct_scalar(node)

        # to find all env variables in line
        if match := pattern.findall(value):
            full_value = value
            for g in match:
                curr_default_value = default_value
                env_var_name = g
                env_var_name_with_default = g
                if default_sep and isinstance(g, tuple) and len(g) > 1:
                    env_var_name = g[0]
                    env_var_name_with_default = ''.join(g)
                    found = False
                    for each in g:
                        if default_sep in each:
                            _, curr_default_value = each.split(default_sep, 1)
                            found = True
                            break
                    if not found and raise_if_na:
                        raise ValueError(
                            f'Could not find default value for {env_var_name}'
                        )
                full_value = full_value.replace(
                    f'${{{env_var_name_with_default}}}',
                    os.environ.get(env_var_name, curr_default_value)
                )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    if path:
        with open(path, mode='r', encoding=encoding) as conf_data:
            if memory_read:
                with mmap.mmap(conf_data.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                    return yaml.load(mmap_obj, Loader=loader)
            return yaml.load(conf_data, Loader=loader)
    elif data:
        return yaml.load(data, Loader=loader)
    else:
        raise ValueError('Either a path or data should be defined as input')


RE_LINE = re.compile(r"""
    ^
    (?:export\s+)?      # optional export
    ([\w\.]+)           # key
    (?:\s*=\s*|:\s+?)   # separator
    (                   # optional value begin
        '(?:\'|[^'])*'  #   single quoted value
        |               #   or
        "(?:\"|[^"])*"  #   double quoted value
        |               #   or
        [^#\n]+         #   unquoted value
    )?                  # value end
    (?:\s*\#.*)?        # optional comment
    $
""", re.VERBOSE)

RE_VARIABLE = re.compile(r"""
    (\\)?               # is it escaped with a backslash?
    (\$)                # literal $
    (                   # collect braces with var for sub
        \{?             #   allow brace wrapping
        ([A-Z0-9_]+)    #   match the variable
        \}?             #   closing brace
    )                   # braces end
""", re.IGNORECASE | re.VERBOSE)


def load_dotenv(
        dotenv: Optional[str] = None,
        override: bool = False
) -> None:
    """
    Read a .env files into os.environ.

    If not given a path to a dotenv path, does filthy magic stack backtracking
    to find manage.py and then find the dotenv.

    If tests rely on .env files, setting the overwrite flag to True is a safe
    way to ensure tests run consistently across all environments.

    :param dotenv: path of dotenv
    :param override: True if values in .env should override system variables.
    """
    if dotenv is None:
        # TODO: check about method `_getframe()`
        frame_filename = sys._getframe().f_back.f_code.co_filename
        dotenv = os.path.join(os.path.dirname(frame_filename), '.env')

    if os.path.isdir(dotenv) and os.path.isfile(os.path.join(dotenv, '.env')):
        dotenv = os.path.join(dotenv, '.env')

    if os.path.exists(dotenv):
        with open(dotenv, mode='r', encoding='utf-8') as f:
            for k, v in parse_dotenv(f.read()).items():
                # print(f"Set environment {k!r} with value {v!r}")
                if override:
                    os.environ[k] = v
                else:
                    os.environ.setdefault(k, v)
    else:
        warnings.warn("Not reading {0} - it doesn't exist.".format(dotenv),
                      stacklevel=2)


def parse_dotenv(content: str) -> Dict:
    """
    Parse content in dotenv files to dictionary

    :param content:
    """
    env: dict = {}

    for line in content.splitlines():
        if m1 := RE_LINE.search(line):
            key, value = m1.groups()
            if value is None:
                value = ''

            # Remove leading/trailing whitespace
            value = value.strip()

            # Remove surrounding quotes
            if m2 := re.match(r'^([\'"])(.*)\1$', value):
                quote_mark, value = m2.groups()
            else:
                quote_mark = None

            # Unescape all chars except $ so variables can be escaped properly
            if quote_mark == '"':
                value = re.sub(r'\\([^$])', r'\1', value)

            if quote_mark != "'":
                # Substitute variables in a value
                for parts in RE_VARIABLE.findall(value):
                    if parts[0] == '\\':
                        # Variable is escaped, don't replace it
                        replace = ''.join(parts[1:-1])
                    else:
                        # Replace it with the value from the environment
                        replace = env.get(
                            parts[-1],
                            os.environ.get(parts[-1], '')
                        )

                    value = value.replace(''.join(parts[:-1]), replace)

            env[key] = value

        elif not re.search(r'^\s*(?:#.*)?$', line):  # not comment or blank
            warnings.warn(
                "Line {0} doesn't match format".format(repr(line)),
                SyntaxWarning
            )

    return env


if __name__ == '__main__':
    test = parse_config('config.test.yaml')
    print('~'*100)
    print(test)
