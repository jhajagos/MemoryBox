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


def main(connection, meta_data, memory_box_name, item_class_name, data_item_class_name, data_state_names,
         current_state_names, file_name, look_back_until_date="2000-01-01", append_metadata_fields=False):

    schema_dict = {"schema": meta_data.schema, "look_back_until_date": look_back_until_date}

    query_string = """select ti.id as track_item_id, ti.item_class_id, ti.transaction_id, tiu.state_id, 
    s1.name as data_state_name, s2.name as current_state_name,
    di.id as data_item_id, di.data_item_class_id, dic.name as data_item_class_name, data, ti.updated_at, ti.created_at
  from %(schema)s.track_items ti 
  join %(schema)s.item_classes ic on ic.id = ti.item_class_id
  join %(schema)s.memory_boxes mb on mb.id = ic.memory_box_id
  join %(schema)s.track_item_updates tiu on ti.id = tiu.track_item_id
  join 
    (select max(id) as maximum_track_item_update_id from %(schema)s.track_item_updates tu 
      where tu.state_id in (select id from %(schema)s.states where name = any(:data_state_names))
       group by tu.track_item_id) t
        on t.maximum_track_item_update_id = tiu.id
  join %(schema)s.data_items di on di.track_item_update_id = tiu.id
  join %(schema)s.data_item_classes dic on dic.id  = di.data_item_class_id
  join %(schema)s.data_item_types dit on di.data_item_type_id = dit.id
  join %(schema)s.states s1 on tiu.state_id = s1.id
  join %(schema)s.states s2 on ti.state_id = s2.id
  where dic.name = :data_item_class_name and ti.updated_at >= '%(look_back_until_date)s'
    and s2.name = any(:current_state_names)
   order by ti.transaction_id desc, dic.id""" % schema_dict

    sql_parameters = {"memory_box_name": memory_box_name, "item_class_name": item_class_name,
                      "data_item_class_name": data_item_class_name,
                      "data_state_names": data_state_names, "current_state_names": current_state_names}

    cursor = connection.execute(sa.text(query_string), **sql_parameters)
    field_list = []

    for row in cursor:
        if row.data is not None:
            if row.data.__class__ != [].__class__:
                data_elements = [row.data]
            else:
                data_elements = row.data
                
            for element in data_elements:
                for field in element.keys():
                    if field not in field_list:
                        field_list += [field]

    field_list.sort()
    header = field_list

    if append_metadata_fields:
        header += ["_current_state_name", "_data_state_name", "_data_item_class_name", "_position", "_created_at", "_updated_at"]


    with open(file_name, 'wb') as fw:

        csv_writer = csv.writer(fw)
        csv_writer.writerow(header)
        cursor = connection.execute(sa.text(query_string), **sql_parameters)
        for row in cursor:
            data = row.data
            i = 0
            for data_item in data:
                data_row = [''] * len(header)
                for column in data_item:
                    data_row[header.index(column)] = data_item[column]

                if append_metadata_fields:
                    data_row[header.index("_current_state_name")] = row.current_state_name
                    data_row[header.index("_data_state_name")] = row.data_state_name
                    data_row[header.index("_data_item_class_name")] = row.current_state_name
                    data_row[header.index("_position")] = str(i + 1)
                    data_row[header.index("_created_at")] = str(row.updated_at)
                    data_row[header.index("_updated_at")] = str(row.created_at)

                csv_writer.writerow(data_row)
                i += 1


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(
        description="Exports out a tracked item into a CSV file")

    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see './test/testing_config.json.example'",
                               default="./config.json")

    arg_parse_obj.add_argument("-m", "--memory-box-name", dest="memory_box_name", default=None)

    arg_parse_obj.add_argument("-n", "--item-class-name", dest="item_class_name", default=None,
                               help="")

    arg_parse_obj.add_argument("-d", "--data-item-name", dest="data_item_class_name", default=None,
                               help="")

    arg_parse_obj.add_argument("-s", "--state-names", dest="state_names", default=None,
                               help="This is the state/s of the item when the data was extracted")

    arg_parse_obj.add_argument("-t", "--current-state-names", dest="current_state_names", default=None,
                               help="This is the current state of the item"
                               )

    arg_parse_obj.add_argument("-f", "--file-name", dest="file_name", default=None,
                               help="")

    arg_parse_obj.add_argument("-l", "--look-back-until-date", dest="look_back_until_date", default="2000-01-01",
                               help="This is the last date of change that the item has been updated"
                               )

    arg_parse_obj.add_argument("-a", "--append-metadata-fields", dest="append_metadata_fields", default=False,
                               action="store_true")

    arg_obj = arg_parse_obj.parse_args()

    config_json_filename = arg_obj.config_json_filename
    if not os.path.exists(config_json_filename):
        raise IOError, "Configuration file: '%s' does not exist" % config_json_filename

    with open(config_json_filename, "r") as f:
        config_dict = json.load(f)

    connection, meta_data = get_db_connection(config_dict)

    state_names = arg_obj.state_names.split(",")
    current_state_names = arg_obj.current_state_names.split(",")

    main(connection, meta_data, arg_obj.memory_box_name, arg_obj.item_class_name, arg_obj.data_item_class_name,
         state_names, current_state_names, arg_obj.file_name, arg_obj.look_back_until_date,
         arg_obj.append_metadata_fields)