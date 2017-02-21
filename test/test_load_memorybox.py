import unittest
import json
import schema_define
import sqlalchemy as sa

class TestLoadMemoryBox(unittest.TestCase):

    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.meta_data, self.connection)

    def test_build(self):
        self.assertTrue(1)


if __name__ == '__main__':
    unittest.main()

