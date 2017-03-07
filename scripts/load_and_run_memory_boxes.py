import os
import sys
import argparse
import sqlalchemy as sa
import json

try:
    import memorybox as mb
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    import memorybox as mb

from memorybox.schema_define import create_and_populate_schema


def get_db_connection(config_dict, reflect_db=True):
    """Connect to the PostgreSQL database"""
    engine = sa.create_engine(config_dict["connection_uri"])
    connection = engine.connect()
    meta_data = sa.MetaData(connection, schema=config_dict["db_schema"], reflect=reflect_db)

    return connection, meta_data


def initialize_database_schema(config_dict, drop_all_tables=False):
    connection, meta_data = get_db_connection(config_dict, reflect_db=False)

    meta_data, table_dict = create_and_populate_schema(connection, meta_data, drop_all=drop_all_tables)
    print("Initialized %s tables in schema '%s'" % (len(table_dict), meta_data.schema))


def main():
    arg_parse_obj = argparse.ArgumentParser(description="Create and load a MemoryBox for tracking items in an external source")

    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see './test/testing_config.json.example'", default="./config.json")

    arg_parse_obj.add_argument("-j", "--memory-box-json-filename", dest="memory_box_json_filename")

    arg_parse_obj.add_argument("-l", "--list-available-memory-boxes", dest="list_available_memory_boxes",
                               action="store_true", default=False,
                               help="List name of memory boxes that are currently defined")

    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="store_true", default=False,
                               dest="initialize_database_schema",
                               help="In an empty PostGreSQL schema initialize database.")

    arg_parse_obj.add_argument("-d", "--drop-all-tables", action="store_true", default=False,
                               dest="drop_all_tables",
                               help="Drop all tables in schema")

    arg_parse_obj.add_argument("-n", "--item-name", dest="item_name")

    arg_parse_obj.add_argument("-r", "--run-memory-box", action="store_true", help="Run memory box")

    arg_obj = arg_parse_obj.parse_args()

    config_json_filename = arg_obj.config_json_filename
    if not os.path.exists(config_json_filename):
        raise IOError, "Configuration file: '%s' does not exist" % config_json_filename

    with open(config_json_filename, "r") as f:
        config_dict = json.load(f)

    if arg_obj.list_available_memory_boxes or arg_obj.initialize_database_schema:
        pass


if __name__ == "__main__":
    main()