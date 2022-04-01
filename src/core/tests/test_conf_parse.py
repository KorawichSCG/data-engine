import os
import io
import unittest
from ..io.conf_parser import conf

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), 'mockups')


class ConfigWithFileTest(unittest.TestCase):
    TESTDATA_FILENAME_ENV = os.path.join(TESTDATA_DIR, '.test.env')
    STATEMENT_ENV = r"""# Comment on header of file
# This is test `.env` file
export PROJ_NAME='data-engine'
PROJ_AUTH=korawica@email.com
DB_CONN=sql://username:p@ssw0rd@host:port/database
SINGLE_QOUTE='escape\'single-qoute\''
DOUBLE_QOUTE="escape\"double-qoute\""
PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
MULTILINE DATA WITH STRING ST
KEY 49bSDf07bsd03oc6vsAE2sFvc
-----END DSA PRIVATE KEY-----"
SECRET_KEY=YOURSECRETKEYGOESHERE  # Comment inline
SECRET_HASH="something-with-a-\#-hash"
PROJ_PATH=C:\\root
DETAIL="${PROJ_PATH}\${SECRET_KEY}"
CONF_PATH='${PROJ_PATH}\${SECRET_KEY}'
JSON_DATA={"foo": "bar"}
ARRAY_DATA=(01 02 03)
FOO=" some value with space "
BACKICKS=`backicks`"""

    STATEMENT_YAML = r"""# -- utf-8
# Comment in header of file
project_name: "${PROJ_NAME}"  # Comment inline
project_env: "${PROJ_ENV:sandbox}"
statement:
     select:
          json_data: '{ "customer": "John Doe", "items": {"product": "Beer","qty": 6}}'
     from: "{{ @table_name {inbound_param01} }}"
     where: "{{ @condition }}"
datasets:
     postgres.sandbox:
          conn: ${DB_CONN:psycog://scghifaai:scghretail#1234@host.ap-southeast-1.rds.amazonaws.com/scgh_dev_db}
host: $REDIS_HOST|127.0.0.1
port: ${PORT|5432}
database: ${DB_NAME}_test
query: |-
      SELECT * FROM "users" WHERE "user" = $1 AND "login" = $2 AND "pwd" = $3
escape_again: $$$$TEST2
escape_again2: $TEST2"""

    def setUp(self) -> None:
        if not os.path.exists(TESTDATA_DIR):
            os.mkdir(TESTDATA_DIR)
        with io.open(os.path.join(TESTDATA_DIR, '.test.env'), mode='w') as file:
            file.write(self.STATEMENT_ENV)

        with io.open(os.path.join(TESTDATA_DIR, 'config.yaml'), mode='w') as file:
            file.write(self.STATEMENT_YAML)

        conf.load_env(os.path.join(TESTDATA_DIR, '.test.env'))
        self.conf_set_file_name = conf.load(os.path.join(TESTDATA_DIR, 'config.yaml'))

    def tearDown(self) -> None:
        os.remove(os.path.join(TESTDATA_DIR, '.test.env'))
        os.remove(os.path.join(TESTDATA_DIR, 'config.yaml'))
        os.rmdir(TESTDATA_DIR)

    def test_conf_env_file(self):
        _value_dict = os.environ.copy()
        self.assertEqual(_value_dict['PROJ_NAME'], 'data-engine')
        self.assertEqual(_value_dict['PROJ_AUTH'], 'korawica@email.com')
        self.assertEqual(_value_dict['DB_CONN'], 'sql://username:p@ssw0rd@host:port/database')
        self.assertEqual(_value_dict['SINGLE_QOUTE'], r"escape\'single-qoute\'")
        self.assertEqual(_value_dict['DOUBLE_QOUTE'], 'escape"double-qoute"')
        self.assertEqual(_value_dict['PRIVATE_KEY'], '-----BEGIN RSA PRIVATE KEY-----'
                                                     'MULTILINE DATA WITH STRING ST'
                                                     'KEY 49bSDf07bsd03oc6vsAE2sFvc'
                                                     '-----END DSA PRIVATE KEY-----')
        self.assertEqual(_value_dict['SECRET_KEY'], 'YOURSECRETKEYGOESHERE')
        self.assertEqual(_value_dict['SECRET_HASH'], 'something-with-a-#-hash')
        self.assertEqual(_value_dict['PROJ_PATH'], r'C:\\root')
        self.assertEqual(_value_dict['DETAIL'], r'C:\\root${SECRET_KEY}')
        self.assertEqual(_value_dict['CONF_PATH'], r'${PROJ_PATH}\${SECRET_KEY}')
        self.assertEqual(_value_dict['JSON_DATA'], '{"foo": "bar"}')
        self.assertEqual(_value_dict['ARRAY_DATA'], '(01 02 03)')
        self.assertEqual(_value_dict['FOO'], ' some value with space ')
        self.assertEqual(_value_dict['BACKICKS'], 'backicks')

    def test_conf_yaml_file(self):
        self.assertEqual(self.conf_set_file_name['project_name'], 'data-engine')
        self.assertEqual(self.conf_set_file_name['project_env'], 'sandbox')
        self.assertEqual(self.conf_set_file_name['escape_again'], '$$TEST2')


if __name__ == '__main__':
    unittest.main()
