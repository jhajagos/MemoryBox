from sqlalchemy import Table, Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import JSONB


def schema_define(meta_data):
    """Define database schema"""

    data_connection_types = Table("data_connection_types", meta_data,
                                  Column("id", Integer, primary_key=True),
                                  Column("parent_id", ForeignKey("data_connection_types.id")),
                                  Column("name", String(255), unique=True))

    data_connections = Table("data_connections", meta_data,
                             Column("id", Integer, primary_key=True),
                             Column("name", String(255), unique=True),
                             Column("data_connection_type_id", ForeignKey("data_connection_types.id")))

    memory_boxes = Table("memory_boxes", meta_data,
                         Column("id", Integer, primary_key=True),
                         Column("name", String(255), unique=True),
                         Column("data_connection_id", ForeignKey("data_connections.id")))

    query_templates = Table("query_templates", meta_data,
                            Column("id", Integer, primary_key=True),
                            Column("name", String(255), unique=True),
                            Column("template", Text),
                            Column("parameter_list", JSONB))

    item_classes = Table("item_classes", meta_data,
                         Column("id", Integer, primary_key=True),
                         Column("name", String(255), unique=True),
                         Column("memory_box_id", ForeignKey("memory_boxes.id")))

    states=Table("states", meta_data,
                     Column("id", Integer, primary_key=True),
                     Column("name", String(255), unique=True))

    actions = Table("actions", meta_data,
                           Column("id", Integer, primary_key=True),
                           Column("name", String(255), unique=True))

    transition_state_item_classes = Table("transition_state_item_classes", meta_data,
                                           Column("id", Integer, primary_key=True),
                                           Column("from_state_id", ForeignKey("states.id")),
                                           Column("to_state_id", ForeignKey("states.id")),
                                           Column("action_id", ForeignKey("actions.id")),
                                           Column("item_class_id", ForeignKey("item_classes.id")),
                                           Column("query_template_id", ForeignKey("query_templates.id")),
                                           Column("parameters", JSONB),
                                           Column("defaults", JSONB)
                                          )

    track_items = Table("track_items", meta_data,
                        Column("id", Integer, primary_key=True),
                        Column("item_class_id", ForeignKey("item_classes.id")),
                        Column("state_id", ForeignKey("states.id")),
                        Column("transaction_id", String(255), index=True),
                        Column("created_at", DateTime),
                        Column("updated_at", DateTime))

    track_item_updates = Table("track_item_updates", meta_data,
                               Column("id", Integer, primary_key=True),
                               Column("track_item_id", ForeignKey("track_items.id")),
                               Column("state_id", ForeignKey("states.id")),
                               Column("created_at", DateTime))

    data_item_type = Table("data_item_types", meta_data,
                           Column("id", Integer, primary_key=True),
                           Column("name", String(255), unique=True))

    data_item_classes = Table("data_item_classes", meta_data,
                              Column("id", Integer, primary_key=True),
                              Column("name", String(255), unique=True),
                              Column("data_item_type_id", ForeignKey("data_item_types.id")))

    data_item_classes_actions = Table("data_item_class_actions", meta_data,
                                      Column("id", Integer, primary_key=True),
                                      Column("action_id", ForeignKey("actions.id")),
                                      Column("data_item_class_id", ForeignKey("data_item_classes.id")),
                                      Column("query_template_id", ForeignKey("query_templates.id")),
                                      Column("parameters", JSONB))

    data_item_actions_transitions_state_items = Table("data_item_actions_transition_state_items", meta_data,
                                           Column("id", Integer, primary_key=True),
                                           Column("data_item_class_action_id", ForeignKey("data_item_class_actions.id")),
                                           Column("transition_state_item_class_id", ForeignKey("transition_state_item_classes.id")))



    data_items = Table("data_items", meta_data,
                       Column("id", Integer, primary_key=True),
                       Column("data", JSONB),
                       Column("text", Text),
                       Column("base64_binary_file_content", Text),
                       Column("data_item_class_id", ForeignKey("data_item_classes.id")),
                       Column("data_item_type_id", ForeignKey("data_item_types.id")),
                       Column("track_item_update_id", ForeignKey("track_item_updates.id")),
                       Column("created_at", DateTime))

    return meta_data


def get_table_names_without_schema(meta):
    table_dict = {}
    for full_table_name in meta.tables:
        if meta.schema is not None:
            schema, table_name = full_table_name.split(".")
            table_dict[table_name] = full_table_name
        else:
            table_dict[full_table_name] = full_table_name
    return table_dict


def populate_reference_table(table_name, connection, meta, list_of_values):
    table_obj = meta.tables[table_name]

    for tuple_value in list_of_values:
        connection.execute(table_obj.insert(tuple_value))


def create_and_populate_schema(connection, meta_data, drop_all=True):
    meta_data = schema_define(meta_data)

    if drop_all:
        meta_data.drop_all()

    meta_data.create_all(checkfirst=True)

    table_dict = get_table_names_without_schema(meta_data)

    data_item_types = [(1, "JSON"), (2, "Text"), (3, "Base64")]
    populate_reference_table(table_dict["data_item_types"], connection, meta_data, data_item_types)

    states = [(1, "Start"), (2, "Stop"), (3, "Watch"), (4, "Archive"), (5, "New")]
    populate_reference_table(table_dict["states"], connection,  meta_data, states)

    data_connection_types = [(1, None, "Relational database"), (2, 1, "SQLite"), (3, 1, "PostGreSQL")]
    populate_reference_table(table_dict["data_connection_types"], connection,  meta_data, data_connection_types)

    actions = [(1, "Pass"), (2, "Insert"), (3, "Insert new")]
    populate_reference_table(table_dict["actions"],  connection, meta_data, actions)

    return meta_data, table_dict

