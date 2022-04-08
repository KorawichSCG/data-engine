# Reference: https://github.com/fsspec/filesystem_spec
import re
import os
import os.path as osp
from pathlib import Path
from typing import AnyStr, Union, Optional, Any, Dict
from urllib.parse import quote_plus, unquote, parse_qsl
from core.utils import str_to_number, to_list, merge_dicts, memoized_property

RE_DB = re.compile(r"""
    (?P<name>[\w\+]+)    # protocal
    (?:://
        (?P<url>.*)      # main url
    )?
""", re.VERBOSE)


# (?:(?P<username>[^:/]*)(?::(?P<password>[^/]*))?@)?(?:(?P<host>[^/:]*)(?::(?P<port>[^/]*))?)?(?:/(?P<database>.*))?
RE_DB__URL = re.compile(r"""
    (?:
        (?P<username>[^:/]*)
        (?::(?P<password>[^/]*))?
    @)?
    (?:
        (?P<host>[^/:]*)
        (?::(?P<port>[^/]*))?
    )?
    (?:/(?P<database>.*))?
""")


RE_DB_URL = re.compile(r"""
    (?P<name>[\w\+]+)://
    (?:
        (?P<username>[^:/]*)
        (?::(?P<password>[^/]*))?
    @)?
    (?:
        (?P<host>[^/:]*)
        (?::(?P<port>[^/]*))?
    )?
    (?:/(?P<database>.*))?
""", re.VERBOSE)


def path_join(full_path: Union[AnyStr, Path], full_join_path: str, swap_slash: bool = True) -> AnyStr:
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
    return _abspath.replace("\\", "/") if swap_slash else _abspath


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


# (?P<name>[\w\+]+)://(?:(?P<username>[^:/]*)(?::(?P<password>[^@]*))?@)?(?:(?:\[(?P<ipv6host>[^/\?]+)\]|(?P<ipv4host>[^/:\?]+))?(?::(?P<port>[^/\?]*))?)?(?:/(?P<database>[^\?]*))?(?:\?(?P<query>.*))?
# RE_DB_FULL = re.compile(r"""
#     (?P<name>[\w\+]+)://
#     (?:
#         (?P<username>[^:/]*)
#         (?::(?P<password>[^@]*))?
#     @)?
#     (?:
#         (?:
#             \[(?P<ipv6host>[^/\?]+)\]
#             |
#             (?P<ipv4host>[^/:\?]+)
#         )?
#         (?::(?P<port>[^/\?]*))?
#     )?
#     (?:/(?P<database>[^\?]*))?
#     (?:\?(?P<query>.*))?
# """, re.VERBOSE)

# (?P<protocol>[\w+]+)?(?:://(?P<host_url>[^/]*))?(?:/(?P<path_url>.*))?
RE_DB_PROTOCOL = re.compile(r"""
    (?P<protocol>[\w+]+)?       # protocal
    (?:://
        (?P<host_url>[^/]*)     # host url contain username/password
    )?
    (?:/
        (?P<path_url>.*)        # path url contain url/query string
    )?
""", re.VERBOSE)

RE_DB_HOST = re.compile(r"""
    (?:
        (?P<username>[^:/]*)
        (?::(?P<password>[^@]*))?
    @)?
    (?:
        (?:
            \[(?P<ipv6host>[^/?]+)\]
            |
            (?P<ipv4host>[^/:?]+)
        )?
        (?::(?P<port>[^/?]*))?
    )?
""", re.VERBOSE)

# (?:(?P<username>[^:/@]*)@)?(?:(?P<host>[^/\?]+))?(?:/(?P<database>[^\?]*))?(?:\?(?P<query>.*))?
RE_DB_PATH = re.compile(r"""
    (?P<database>[^?]*)
    (?:\?
        (?P<query>.*)
    )?
""", re.VERBOSE)


class ParseURLs:
    """
    Class of parse string url content
    """
    def __init__(
            self,
            url: str
    ):
        _url_convert: dict = self._parse_urls(self._parse_protocol(url))
        self.protocol = _url_convert['protocol']
        self.username = _url_convert['username']
        self.password = _url_convert['password']
        self.host = _url_convert['host']
        self.port: Optional[int] = _url_convert['port']
        self.database = _url_convert['database']
        self.query = _url_convert.get('query', {})

    @memoized_property
    def normalized_query(self):
        return {k: v if isinstance(v, tuple) else (v,) for k, v in self.query.items()}

    def render_as_string(self, hide_password: bool = True) -> str:
        """Render this :class:`_engine.URL` object as a string.

        This method is used when the ``__str__()`` or ``__repr__()``
        methods are used.   The method directly includes additional options.

        :param hide_password: Defaults to True.   The password is not shown
         in the string unless this is set to False.

        """
        s = f'{self.protocol}://'
        if self.username is not None:
            s += self._quote(self.username)
            if self.password is not None:
                s += ":" + ("***" if hide_password else self._quote(str(self.password)))
            s += "@"
        if self.host is not None:
            s += f"[{self.host}]" if ":" in self.host else self.host
        if self.port is not None:
            s += f":{str(self.port)}"
        if self.database is not None:
            s += f"/{self.database}"
        if self.query:
            keys = sorted(self.query)
            s += "?" + "&".join(
                f"{quote_plus(k)}={quote_plus(element)}" for k in keys for element in to_list(self.query[k])
            )
        return s

    def __str__(self) -> str:
        return self.render_as_string(hide_password=False)

    def __repr__(self) -> str:
        return self.render_as_string()

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ParseURLs)
            and self.protocol == other.protocol
            and self.username == other.username
            and self.password == other.password
            and self.host == other.host
            and self.database == other.database
            and self.query == other.query
            and self.port == other.port
        )

    def __ne__(self, other: Any) -> bool:
        return not self == other

    @staticmethod
    def _parse_urls(content_dict: dict):
        if username := content_dict["username"]:
            content_dict["username"] = unquote(username)
        if password := content_dict["password"]:
            content_dict["password"] = unquote(password)
        content_dict["port"] = str_to_number(content_dict["port"])
        return content_dict

    @staticmethod
    def _parse_protocol(content: str) -> Dict[str, Union[str, int, dict]]:
        """
        Parse full url and split to protocol, host, and path
        """
        if not (m := RE_DB_PROTOCOL.match(content)):
            raise ValueError(f"Could not parse URL from string {content!r}")
        match_ptc: dict = m.groupdict()
        _mapping: Dict[str, Union[str, int, dict]] = {
            'protocol': match_ptc['protocol'],
            'username': None, 'password': None, 'host': None, 'port': None
        }
        if path_url := match_ptc['path_url']:
            match_path_url = RE_DB_PATH.match(path_url).groupdict()
            _mapping['database'] = match_path_url["database"]
            if _query := match_path_url["query"]:
                query: dict = {}
                for key, value in parse_qsl(_query):
                    if key in query:
                        query[key]: list = to_list(query[key])
                        query[key].append(value)
                    else:
                        query[key] = value
                _mapping["query"] = query
        if host_url := match_ptc['host_url']:
            match_path_host = RE_DB_HOST.match(host_url).groupdict()
            ipv4host = match_path_host.pop("ipv4host")
            ipv6host = match_path_host.pop("ipv6host")
            _mapping["host"] = ipv4host or ipv6host
            _mapping = merge_dicts(_mapping, match_path_host)
        print(_mapping)
        return _mapping

    @staticmethod
    def _quote(text: str) -> str:
        return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)

    @staticmethod
    def _unquote(text: str) -> str:
        return unquote(text)


urls: callable = ParseURLs

__all__ = ['urls']


if __name__ == '__main__':
    url1 = ParseURLs('pyodbc://korawica@scg.com:${DB_MSS_PASS}@${DB_MSS_HOST}/DWHCTRLDEV?driver=ODBC+Driver+17+for+SQL+Server')
    url2 = ParseURLs('sqlit:///${PROJ_PATH}/data/sandbox/scgh_dev.db')
    url3 = ParseURLs('postgres+psycogpg://scghifaai:${DB_PG_PASS}@${DB_PG_HOST}:5432/scgh_dev_db')
    print(url1.normalized_query)
    print(url1.normalized_query)
    # print(quote_plus('ODBC Driver 17 for SQL Server'))
