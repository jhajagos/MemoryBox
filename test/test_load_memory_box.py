import unittest
import json
import schema_define
import sqlalchemy as sa

from load import MemoryBoxLoader

class TestLoadMemoryBox(unittest.TestCase):

    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.meta_data, self.connection)

        with open("./files/encounter_memory_box_test_load.json") as f:
            self.memory_box_struct = json.load(f)

    def test_load(self):

        mbox_load_obj = MemoryBoxLoader(self.meta_data, self.connection, self.memory_box_struct)

        row_obj_1 = self.connection.execute("select * from %s.memory_boxes" % self.meta_data.schema)
        self.assertEqual(0, len(list(row_obj_1)))

        mbox_load_obj.load_into_db()
        row_obj_2 = self.connection.execute("select * from %s.memory_boxes" % self.meta_data.schema)
        self.assertEqual(1, len(list(row_obj_2)))



if __name__ == '__main__':
    unittest.main()

