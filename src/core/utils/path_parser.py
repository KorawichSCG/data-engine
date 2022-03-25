import os
from pathlib import Path
from typing import AnyStr, Union


def path_join(full_path: Union[AnyStr, Path], full_join_path: str) -> AnyStr:
    """
    Join path with multi pardir value. This function base on `os.path.join`

    Example
    -------
        `full_join_path` is '../../<path>'
    """
    _abspath: Union[AnyStr, Path] = Path('.').resolve() if full_path == '.' else full_path
    _join_split: list = os.path.normpath(full_join_path).split(os.sep)
    for path in _join_split:
        _abspath: AnyStr = os.path.abspath(os.path.join(_abspath, os.pardir)) if path == '..' \
            else os.path.abspath(os.path.join(_abspath, path))
    return _abspath


# TODO: Upgrade path join to version 2
def path_join2(path: AnyStr, path_join: str) -> AnyStr:
    """
    Join path with multi pardir value
    :param str path:
    :param str path_join:
    :return:
    """
    # path = (Path(__file__).parent).joinpath('.env')
    _path = Path.joinpath(Path(path), path_join)
    print(path)
    print(_path)
    return _path


def walk(path):
    for _p in Path(path).iterdir():
        if _p.is_dir():
            yield from walk(_p)
            continue
        yield _p.resolve()


if __name__ == '__main__':
    # path_join2(__file__, '../..')
    print('.../test/main'.rsplit('/', 1))
    print('.../test/main'.rsplit('/'))

    # recursively traverse all files from current directory
    for p in walk(Path('')):
        print(p)

    # the function returns a generator so if you need a list you need to build one
    all_files = list(walk(Path('')))
