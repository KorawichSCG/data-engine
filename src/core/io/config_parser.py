# Reference: https://github.com/mkaranasou/pyaml_env
import re
import os
import yaml
import mmap
from typing import Dict, Any


class BaseConfig:
    """
    Example
    -------
        path_data = 'local_file: path: "data/local/file.csv"'
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
    Load yaml configuration from path or from the contents of a file (data)
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
    :param str path: the path to the yaml file
    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for, if None, all env variables will be
        resolved.
    :param str default_sep: if any default values are set, use this field
        to separate them from the enironment variable name. E.g. ':' can be
        used.
    :param str default_value: the tag to look for
    :param bool raise_if_na: raise an exception if there is no default
        value set for the env variable.
    :param bool memory_read: the flag of read file by `mmap`
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
