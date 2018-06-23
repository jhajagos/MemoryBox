import unittest
from test_utilities import load_csv_into_database, create_schema_test_database
import schema_define
import sqlalchemy as sa
import os
import json
from memorybox.load import MemoryBoxLoader
from memorybox.run import MemoryBoxRunner
import time

class RunMemoryBox(unittest.TestCase):

    def _get_changes_track_id(self):
        q = """
            select 
      tiu.track_item_id, tiu.id as track_item_update_id,
      tiu.state_id, s.name as state_name, tiu.created_at from %s.track_item_updates tiu join %s.states s on s.id = tiu.state_id
    order by track_item_id, tiu.id;
            """ % (self.meta_data.schema, self.meta_data.schema)

        cursor = self.connection.execute(q)
        results = list(cursor)

        track_dict = {}
        for row in results:
            track_item_id = row.track_item_id
            state_name = row.state_name
            if track_item_id not in track_dict:
                track_dict[track_item_id] = [state_name]
            else:
                track_dict[track_item_id] += [state_name]

        return track_dict

    def _get_changes(self):

        schem_dict = {"schema": self.meta_data.schema}

        q = """
        
select cdi.*, dic.name as data_item_class_name, d1.track_item_update_id, track_item_id, ti.transaction_id,
  d1.sha1 as initial_sha1, d2.sha1, d1.data_for_diff as initial_data_for_diff, 
  d2.data_for_diff, d1.text as initial_text, d2.text, ic.name as item_class_name, mb.name as memory_box_name
from 
  %(schema)s.changed_data_items cdi 
  join %(schema)s.data_items d1 on d1.id = cdi.initial_data_item_id
  join %(schema)s.data_items d2 on d2.id = cdi.changed_data_item_id
  join %(schema)s.data_item_classes dic on d1.data_item_class_id = dic.id
  join %(schema)s.track_item_updates tiu on d1.track_item_update_id = tiu.id
  join %(schema)s.track_items ti on ti.id = tiu.track_item_id
  join %(schema)s.item_classes ic on ic.id = ti.item_class_id
  join %(schema)s.memory_boxes mb on mb.id = ic.memory_box_id
        """ % schem_dict

        c = self.connection.execute(q)
        return list(c)


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
                                                                   source_connection, source_meta_data, "updated_at")
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

            schema_define.create_and_populate_schema(self.connection, self.meta_data)

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
                                                                  self.source_connection, self.source_meta_data,
                                                                  "updated_at"
                                                                  )

        self.number_of_second_documents = load_csv_into_database("encounter_documents",
                                                                 "./files/encounter_documents_second_batch.csv",
                                                                 self.source_connection, self.source_meta_data
                                                                 )
        self.memory_box_runner.run("discharges")

        self.source_connection.execute("delete from encounters")
        self.source_connection.execute("delete from encounter_documents")

        self.number_of_third_encounters = load_csv_into_database("encounters",
                                                                 "./files/encounters_third_batch.csv",
                                                                 self.source_connection, self.source_meta_data,
                                                                 "updated_at"
                                                                 )

        self.number_of_third_documents = load_csv_into_database("encounter_documents",
                                                                 "./files/encounter_documents_third_batch.csv",
                                                                 self.source_connection, self.source_meta_data)

        self.memory_box_runner.run("discharges")

        print("Sleeping")
        time.sleep(1) # Test age out

        self.memory_box_runner.run("discharges")

        self.memory_box_runner.run("discharges")

        tracks_dict = self._get_changes_track_id()

        self.assertEquals(4, len(tracks_dict))

        track_1 = tracks_dict[1]
        track_2 = tracks_dict[2]
        track_3 = tracks_dict[3]
        track_4 = tracks_dict[4]

        self.assertEqual(u"New", track_1[0])
        self.assertEqual(u"Archive", track_1[-1])

        self.assertEqual(u"New", track_4[0]) # Test for timeout
        self.assertEqual(u"Archive", track_4[-1])

        changes = self._get_changes()

        self.assertNotEqual(0, len(changes))


if __name__ == '__main__':

    unittest.main()
