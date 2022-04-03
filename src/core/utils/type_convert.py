import ast
import re
from typing import Union
from distutils.util import strtobool


def str_to_bool(content: Union[str, bool], force: bool = True) -> bool:
    """
    Convert string content to boolean type that mean `True` values are y, yes, t, true, on and 1 and
    False values are n, no, f, false, off and 0. Raises ValueError if val is anything else.
    """
    if isinstance(content, str):
        try:
            return bool(strtobool(content))
        except ValueError:
            return False if force else bool(strtobool(content))
    return content


def str_to_number(content: Union[str, int, float], force_int: bool = False) -> Union[int, float]:
    """
    Convert string content to integer or float type
    """
    if isinstance(content, str):
        try:
            return int(content, 0)
        except ValueError:
            value = float(re.sub(r'[^.\-\d]', '', content))
            return int(value + 0.5) if force_int else value
    return int(content + 0.5) if force_int else content


def str_to_list(content: Union[str, list, dict]) -> Union[list, dict]:
    """
    Convert string content to its literal
    usage
    -----
        >> str_to_list("['2021-01-02', '2021-01-03']")
        ['2021-01-02', '2021-01-03']
    """
    # TODO: add error handler for result of convert does not be list or dict
    # TODO: or add regular expression for cache the correct content like `['fdafd, asdfa'] -> ['fdafd', 'asdfa']
    return ast.literal_eval(content) if isinstance(content, str) else content


# This function alias for dict because it have same logic with `str_to_list`
str_to_dict: callable = str_to_list


def dict_to_str(content: dict, str_pair: str = '=') -> str:
    return f'{{{", ".join(f"{k!r}{str_pair}{v!r}" for k, v in sorted(content.items()))}}}'


def must_pop_list(dictionary, content) -> list:
    return _return_key if isinstance((_return_key := dictionary.pop(content, [])), list) else [_return_key]
