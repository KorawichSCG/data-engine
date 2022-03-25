# Reference: https://github.com/theskumar/python-dotenv
# Reference: https://github.com/jpadilla/django-dotenv
import os
import re
import sys
import warnings
from typing import Dict, Optional

line_re = re.compile(r"""
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

variable_re = re.compile(r"""
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
    Read a .env file into os.environ.

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
    Parse content in dotenv file to dictionary

    :param content:
    """
    env: dict = {}

    for line in content.splitlines():
        if m1 := line_re.search(line):
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
                for parts in variable_re.findall(value):
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
