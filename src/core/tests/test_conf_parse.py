import os
import unittest
from ..io.conf_parser import conf

TESTDATA_FILENAME = os.path.join(os.path.dirname(__file__), 'mockups/.test.env')


class ConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        conf.load_env(TESTDATA_FILENAME)

    def tearDown(self) -> None:
        pass

    def test_default_mapping(self):
        _value_dict = os.environ.copy()
        self.assertEqual(_value_dict['PROJ_NAME'], 'data-engine')


if __name__ == '__main__':
    unittest.main()
