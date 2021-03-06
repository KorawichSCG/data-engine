import sys
import ast
import math
import hashlib
from typing import AnyStr, Iterable, List, Optional, Union
from distutils.util import strtobool
import re
import pandas as pd
import string
import random
import importlib
from collections import defaultdict

# __all__ = ['split_iterable', 'merge_dicts', 'hash_string', 'import_string']


def cached_import(module_path, class_name):
    modules = sys.modules
    if module_path not in modules or (
            # Module is not fully initialized.
            getattr(modules[module_path], '__spec__', None) is not None and
            getattr(modules[module_path].__spec__, '_initializing', False) is True
    ):
        importlib.import_module(module_path)
    return getattr(modules[module_path], class_name)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(
            'Module "%s" does not define a "%s" attribute/class' % (module_path, class_name)
        ) from err


def split_str(source, sep: Optional[str] = None):
    """
    warning: does not yet work if sep is a lookahead like `(?=b)`
    usage:
        >> splitStr('.......A...b...c....', sep='...')
        <generator object splitStr.<locals>.<genexpr> at 0x7fe8530fb5e8>

        >> list(splitStr('A,b,c.', sep=','))
        ['A', 'b', 'c.']

        >> list(splitStr(',,A,b,c.,', sep=','))
        ['', '', 'A', 'b', 'c.', '']

        >> list(splitStr('.......A...b...c....', '\.\.\.'))
        ['', '', '.A', 'b', 'c', '.']

        >> list(splitStr('   A  b  c. '))
        ['', 'A', 'b', 'c.', '']
    """
    sep = sep or "\s+"
    if sep == '':
        return iter(source)
    # return (_.group(1) for _ in re.finditer(f'(?:^|{sep})((?:(?!{sep}).)*)', source))
    # alternatively, more verbosely:
    regex = f'(?:^|{sep})((?:(?!{sep}).)*)'
    for match in re.finditer(regex, source):
        yield match.group(1)


def isplit(source, sep=None, regex=False):
    """
    generator version of str.split()

    :param source:
        source string (unicode or bytes)

    :param sep:
        separator to split on.

    :param regex:
        if True, will treat sep as regular expression.

    :returns:
        generator yielding elements of string.

    usage:
        >> print list(isplit("abcb","b"))
        ['a','c','']
    """
    if sep is None:
        # mimic default python behavior
        source = source.strip()
        sep = "\\s+"
        if isinstance(source, bytes):
            sep = sep.encode("ascii")
        regex = True
    start = 0
    if regex:
        # version using re.finditer()
        if not hasattr(sep, "finditer"):
            sep = re.compile(sep)
        for m in sep.finditer(source):
            idx = m.start()
            assert idx >= start
            yield source[start:idx]
            start = m.end()
        yield source[start:]
    else:
        # version using str.find(), less overhead than re.finditer()
        sep_size = len(sep)
        while True:
            idx = source.find(sep, start)
            if idx == -1:
                yield source[start:]
                return
            yield source[start:idx]
            start = idx + sep_size


def split_iterable(iterable, chunk_size=None, generator_flag=None):
    """
    Split an iterable into mini batch with batch length of batch_number
    supports batch of a pandas dataframe
    usage:
        >> for i in split_iterable([1,2,3,4,5], chunk_size=2):
        >>    print(i)
        [1, 2]
        [3, 4]
        [5]

        for idx, mini_data in split_iterable(batch(df, chunk_size=10)):
            print(idx)
            print(mini_data)
    """
    chunk_size: int = chunk_size or 25000
    generator_flag: bool = generator_flag or True
    num_chunks = math.ceil(len(iterable) / chunk_size)
    if generator_flag:
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                yield iterable.iloc[_ * chunk_size:(_ + 1) * chunk_size]
            else:
                yield iterable[_ * chunk_size:(_ + 1) * chunk_size]
    else:
        chunks: list = []
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                chunks.append(iterable.iloc[_ * chunk_size:(_ + 1) * chunk_size])
            else:
                chunks.append(iterable[_ * chunk_size:(_ + 1) * chunk_size])
        return chunks


def merge_dicts(*dict_args) -> dict:
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    usage:
            >> print(merge_dicts({1: 'one',2: 'two',3: 'three'}, {3: 'Three',4: 'Four'}))
            {1: 'one', 2: 'two', 3: 'Three', 4: 'Four'}
    """
    result: dict = {}
    for dictionary in dict_args:
        if dictionary:
            result.update(dictionary)
    return result


def hash_string(input_value: str, num_length: int = 8, method='sha256') -> str:
    """
    hash str function input to number with default method, SHA256 algorithm.
    """
    return str(int(getattr(hashlib, method)(input_value.encode('utf-8')).hexdigest(), 16) % 10 ** num_length)


def random_sting(num_length: int = 8) -> str:
    """
    random string from uppercase ASCII and number 0-9
    """
    # TODO: dynamic random with input group of string such as __lower__, __special__, __upper__
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=num_length))


def convert_str_list_to_list(str_list: str) -> list:
    """
    Get list of run_date from list string of run_date
    usage
    -----
        >> print(convert_str_list_to_list("['2021-01-02', '2021-01-03']"))
        ['2021-01-02', '2021-01-03']
    """
    return ast.literal_eval(str_list)


def sort_by_priority_list(values: Iterable, priority: List) -> List:
    """
    Sorts an iterable according to a list of priority items.
    Usage
    -----
        >> sort_by_priority_list(values=[1,2,2,3], priority=[2,3,1])
        [2, 2, 3, 1]
        >> sort_by_priority_list(values=set([1,2,3]), priority=[2,3])
        [2, 3, 1]
    """
    # priority_dict = {k: i for i, k in enumerate(priority)}
    #
    # def priority_getter(value):
    #     return priority_dict.get(value, len(values))
    #
    # return sorted(values, key=priority_getter)
    priority_dict = defaultdict(
        lambda: len(priority), zip(priority, range(len(priority)), ),
    )
    priority_getter = priority_dict.__getitem__  # dict.get(key)
    return sorted(values, key=priority_getter)


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
