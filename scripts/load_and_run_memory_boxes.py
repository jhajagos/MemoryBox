import memorybox as mb
import argparse


def main():
    arg_parse_obj = argparse.ArgumentParser(description="Create and load a MemoryBox for tracking items in an external source")

    arg_parse_obj.add_parameter()
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see './test/testing_config.json.example'", default="./config.json")

    arg_parse_obj.add_argument("-j", "--memory-box-json-filename", dest="memory_box_json_filename")

    arg_parse_obj.add_argument("-r", "--run-memory-box", dest="run_memory_box")

    arg_parse_obj.add_argument("-l", "--list-available-memory-boxes", dest="list_available_memory_boxes",
                               action="store_true", default=False,
                               help="List name of memory boxes that are currently defined")

    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="store_true", default=False,
                               dest="initialize_database_schema",
                               help="In an empty PostGreSQL schema initialize database.")

    arg_parse_obj.add_argument("-d", "--drop-all-tables", action="store_true", default=False,
                               dest="drop_all_tables",
                               help="Drop all tables in schema")

    arg_parse_obj.add_argument("-i", "--item-name", dest="item_name")

    arg_parse_obj.add_argument("-r", "--run-memory-box", action="store_true", help="Run memory box")

    arg_obj = argparse.parse_args()

if __name__ == "__main__":
    main()