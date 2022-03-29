# Reference: https://github.com/fsspec/filesystem_spec
import re
import os
import os.path as osp
from pathlib import Path
from typing import AnyStr, Union


def path_join(full_path: Union[AnyStr, Path], full_join_path: str) -> AnyStr:
    """
    Join path with multi pardir value. This function base on `os.path.join`

    Example
    -------
        `full_join_path` is '../../<path>'

    Upgrade
    -------
        goal_dir = os.path.join(os.getcwd(), "../../my_dir")
        print goal_dir  # prints C:/here/I/am/../../my_dir
        print os.path.normpath(goal_dir)  # prints C:/here/my_dir
        print os.path.realpath(goal_dir)  # prints C:/here/my_dir
        print os.path.abspath(goal_dir)  # prints C:/here/my_dir
    """
    _abspath: Union[AnyStr, Path] = Path('../utils').resolve() if full_path == '.' else full_path
    _join_split: list = os.path.normpath(full_join_path).split(os.sep)
    for path in _join_split:
        _abspath: AnyStr = os.path.abspath(os.path.join(_abspath, os.pardir)) if path == '..' \
            else os.path.abspath(os.path.join(_abspath, path))
    return _abspath


def walk(path: Union[AnyStr, Path]):
    for _p in Path(path).iterdir():
        if _p.is_dir():
            yield from walk(_p)
            continue
        yield _p.resolve()


def make_path_posix(path, sep=os.sep):
    """Make path generic"""
    if isinstance(path, (list, set, tuple)):
        return type(path)(make_path_posix(p) for p in path)
    if "~" in path:
        path = osp.expanduser(path)
    if sep == "/":
        # most common fast case for posix
        return path if path.startswith("/") else f'{os.getcwd()}/{path}'
    if (
        (sep not in path and "/" not in path)
        or (sep == "/" and not path.startswith("/"))
        or (sep == "\\" and ":" not in path and not path.startswith("\\\\"))
    ):
        # relative path like "path" or "rel\\path" (win) or rel/path"
        if os.sep == "\\":
            # abspath made some more '\\' separators
            return make_path_posix(osp.abspath(path))
        else:
            return f'{os.getcwd()}/{path}'
    if re.match("/[A-Za-z]:", path):
        # for windows file URI like "file:///C:/folder/file"
        # or "file:///C:\\dir\\file"
        path = path[1:]
    if path.startswith("\\\\"):
        # special case for windows UNC/DFS-style paths, do nothing,
        # just flip the slashes around (case below does not work!)
        return path.replace("\\", "/")
    if re.match("[A-Za-z]:", path):
        # windows full path like "C:\\local\\path"
        return path.lstrip("\\").replace("\\", "/").replace("//", "/")
    if path.startswith("\\"):
        # windows network path like "\\server\\path"
        return "/" + path.lstrip("\\").replace("\\", "/").replace("//", "/")
    return path
