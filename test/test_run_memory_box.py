import unittest
from test_utilities import load_csv_into_database, create_schema_test_database
import sqlalchemy as sa
import os


class RunMemoryBox(unittest.TestCase):

    def setUp(self):
        test_connection_string = "sqlite:///./files/test_encounter.db3"
        if os.path.exists("./files/test_encounter.db3"):
            os.remove("./files/test_encounter.db3")

        with open("./files/encounters_database_schema.sql") as f:
            schema_string = f.read()

        connection, meta_data = create_schema_test_database(test_connection_string, schema_string)

        self.number_of_initial_encounters = load_csv_into_database("encounters",
                                                                   "./files/encounters_first_batch.csv",
                                                                   connection, meta_data)
        self.number_of_initial_encounter_dx = load_csv_into_database("encounter_dx",
                                                                     "./files/encounter_dx_first_batch.csv",
                                                                     connection, meta_data)
        self.number_of_initial_persons = load_csv_into_database("patient", "./files/patient_first_batch.csv",
                                                                connection, meta_data)

    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
