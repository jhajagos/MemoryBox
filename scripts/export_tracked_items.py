import argparse
import sqlalchemy as sa
import os
import json
import csv

def get_db_connection(config_dict, reflect_db=True):
    """Connect to the PostgreSQL database"""
    engine = sa.create_engine(config_dict["connection_uri"])
    connection = engine.connect()
    meta_data = sa.MetaData(connection, schema=config_dict["db_schema"], reflect=reflect_db)

    return connection, meta_data

def main(connection, meta_data, memory_box_name, item_class_name, data_item_class_name, state_name, file_name):

    schema_dict = {"schema": meta_data.schema}

    query_string = """select ti.id as track_item_id, ti.item_class_id, ti.transaction_id, ti.state_id, s.name as state_name,
  di.id as data_item_id, di.data_item_class_id, dic.name as data_item_clas_name, data
from %(schema)s.track_items ti 
  join %(schema)s.item_classes ic on ic.id = ti.item_class_id
  join %(schema)s.memory_boxes mb on mb.id = ic.memory_box_id
  join %(schema)s.states s ON s.id = ti.state_id
  join %(schema)s.track_item_updates itu on itu.track_item_id = ti.id and itu.state_id = s.id
  join %(schema)s.data_items di on di.track_item_update_id = itu.id
  join %(schema)s.data_item_classes dic on dic.id = di.data_item_class_id
  where s.name = :state_name and dic.name = :data_item_class_name and mb.name = :memory_box_name and ic.name = :item_class_name
  order by ti.transaction_id desc, dic.id""" % schema_dict

    sql_parameters = {"memory_box_name": memory_box_name, "item_class_name": item_class_name,
                      "data_item_class_name": data_item_class_name, "state_name": state_name}

    cursor = connection.execute(query_string + " limit 1", sql_parameters)
    top_row_list = list(cursor)
    if len(top_row_list):
        top_row = top_row_list[0]

    memory_box_fields = top_row.keys()
    top_row_data = top_row.data
    top_row_fields = top_row_data.keys()
    top_row_fields.sort()

    header = top_row_fields

    with open(file_name, 'wb') as fw:

        csv_writer = csv.writer(fw)
        csv_writer.writerow(top_row_fields)
        cursor = connection.execute(query_string, sql_parameters)
        for row in cursor:
            data = row.data
            for data_item in data:
                row = [''] * len(header)
                for column in data_item:
                    row[header.index(column)] = data_item[column]
                csv_writer.writerow(row)


arg_parse_obj = argparse.ArgumentParser(
    description="Exports out a tracked item")

arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                           help="JSON configuration file: see './test/testing_config.json.example'",
                           default="./config.json")

arg_parse_obj.add_argument("-m", "--memory-box-name", dest="memory_box_name", default=None)


arg_parse_obj.add_argument("-n", "--item-name", dest="item_name", default=None,
                           help="Name of target item")

arg_parse_obj.add_argument("-d", "--data-item-name", dest="data_item_name", default=None,
                           help="")

arg_parse_obj.add_argument("-s", "--state-name", dest="state_name", default=None,
                           help="")

arg_parse_obj.add_argument("-f", "--file-name", dest="file_name", default=None,
                           help="")


arg_obj = arg_parse_obj.parse_args()

config_json_filename = arg_obj.config_json_filename
if not os.path.exists(config_json_filename):
    raise IOError, "Configuration file: '%s' does not exist" % config_json_filename

with open(config_json_filename, "r") as f:
    config_dict = json.load(f)

connection, meta_data = get_db_connection(config_dict)

main(connection, meta_data, arg_obj.memory_box_name, arg_obj.item_class_name, arg_obj.data_item_class_name, arg_obj.state_name, arg_obj.file_name)