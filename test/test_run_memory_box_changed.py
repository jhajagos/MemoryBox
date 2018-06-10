import unittest
from test_utilities import load_csv_into_database, create_schema_test_database
import schema_define
import sqlalchemy as sa
import os
import json
from memorybox.load import MemoryBoxLoader
from memorybox.run import MemoryBoxRunner


class RunMemoryBox(unittest.TestCase):

    def setUp(self):

        # Setup a source database to test the functionality of tracking items in a changing database

        test_connection_string = "sqlite:///./files/test_encounter.db3"
        if os.path.exists("./files/test_encounter.db3"):
            os.remove("./files/test_encounter.db3")

        with open("./files/encounters_database_schema.sql") as f:
            schema_string = f.read()

        source_connection, source_meta_data = create_schema_test_database(test_connection_string, schema_string)

        self.number_of_initial_encounters = load_csv_into_database("encounters",
                                                                   "./files/encounters_first_batch.csv",
                                                                   source_connection, source_meta_data)
        self.number_of_initial_encounter_dx = load_csv_into_database("encounter_dx",
                                                                     "./files/encounter_dx_first_batch.csv",
                                                                     source_connection, source_meta_data)
        self.number_of_initial_persons = load_csv_into_database("patient", "./files/patient_first_batch.csv",
                                                                source_connection, source_meta_data)

        self.number_of_initial_documents = load_csv_into_database("encounter_documents",
                                                                  "./files/encounter_documents_first_batch.csv",
                                                                  source_connection, source_meta_data
                                                                  )

        self.source_connection = source_connection
        self.source_meta_data = source_meta_data

        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

            self.data_connections = config["data_connections"]

            schema_define.create_and_populate_schema(self.connection, self.meta_data, )

        with open("./files/encounter_memory_box_changed.json") as f:
            self.memory_box_struct = json.load(f)
            self.mbox_load_obj = MemoryBoxLoader(self.memory_box_struct, self.connection, self.meta_data)
            self.mbox_load_obj.load_into_db()

    def test_run_memory_box(self):
        self.memory_box_runner = MemoryBoxRunner("encounter", self.connection, self.meta_data, self.data_connections)

        cursor1 = self.connection.execute("select * from %s.track_items" % self.meta_data.schema)
        self.assertFalse(len(list(cursor1)))

        self.memory_box_runner.run("discharges")

        cursor2 = self.connection.execute("select * from %s.track_items" % self.meta_data.schema)
        self.assertTrue(len(list(cursor2)))

        self.source_connection.execute("delete from encounter_documents")
        self.source_connection.execute("delete from encounters")

        self.number_of_second_encounters = load_csv_into_database("encounters",
                                                                  "./files/encounters_second_batch.csv",
                                                                  self.source_connection, self.source_meta_data
                                                                  )

        self.number_of_second_documents = load_csv_into_database("encounter_documents",
                                                                  "./files/encounter_documents_second_batch.csv",
                                                                  self.source_connection, self.source_meta_data
                                                                 )

        self.memory_box_runner.run("discharges")


if __name__ == '__main__':

    unittest.main()
