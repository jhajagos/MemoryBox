from db_classes import *


class MemoryBoxLoader(object):
    """Load a MemoryBox struct into memory"""

    def __init__(self, meta_data, connection, struct_dict):
        self.meta_data = meta_data
        self.connection = connection
        self.struct_dict = struct_dict

    def load_into_db(self):

        # Add query templates
        query_template_obj = QueryTemplates(self.connection, self.meta_data)
        if "query_templates" in self.struct_dict:
            query_templates = self.struct_dict["query_templates"]
            for query_template in query_templates:
                #TODO: Add logic to replace if name exists
                query_template_obj.insert_struct(query_template)

        # Add data connections

        if "data_connection" not in self.struct_dict:
            raise RuntimeError, "Configuration must have a defined 'data_connection'"
        else:
            data_connection = self.struct_dict["data_connection"]
            data_connection_obj = DataConnections(self.connection, self.meta_data)
            data_connection_type_obj = DataConnectionTypes(self.connection, self.meta_data)

            data_connection_dict = {}
            data_connection_dict["name"] = data_connection["name"]
            data_connection_type = data_connection["data_connection_type"]
            data_connection_type_row_dict = data_connection_type_obj.find_one(data_connection_type)
            data_connection_dict["data_connection_type_id"] = data_connection_type_row_dict.id

            data_connection_id = data_connection_obj.insert_struct(data_connection_dict)

        # Add memory_boxes
        if "name" not in self.struct_dict:
            raise RuntimeError, "Configuration must contain name for the memory box"
        else:
            memory_box_name = self.struct_dict["name"]
            memory_box_obj = MemoryBoxes(self.connection, self.meta_data)
            memory_box_id = memory_box_obj.insert_struct({"name": memory_box_name, "data_connection_id": data_connection_id})
            action_obj = Actions(self.connection, self.meta_data)
            # items
            if "items" not in self.struct_dict:
                raise RuntimeError, "Configuration must contain 'items'"
            else:
                items = self.struct_dict["items"]["classes"]

                for item in items:
                    # Add data_items
                    if "data_items" in item:
                        data_item_class_obj = DataItemClasses(self.connection, self.meta_data)
                        data_item_type_obj = DataItemTypes(self.connection, self.meta_data)
                        data_items = item["data_items"]
                        data_item_classes = data_items["classes"]

                        for data_item_class in data_item_classes:

                            data_item_class_name = data_item_class["name"]
                            data_item_class_type_name = data_item_class["data_type"]

                            data_item_class_dict = {"data_item_type_id": data_item_type_obj.find_one(data_item_class_type_name).id,
                                                    "name": data_item_class_name}

                            data_item_class_id = data_item_class_obj.insert_struct(data_item_class_dict)

                            # Add data_item_actions
                            data_item_class_action_obj = DataItemClassActions(self.connection, self.meta_data)
                            if "actions" in data_item_class:
                                data_item_actions = data_item_class["actions"]
                                for data_item_action in data_item_actions:
                                    query_template_id = query_template_obj.find_one(data_item_action["query_template"]).id
                                    name = data_item_action["name"]
                                    parameters = data_item_action["parameters"]
                                    action_id = action_obj.find_one(name).id

                                    data_item_action_dict = {"parameters": parameters,
                                                             "action_id": action_id,
                                                             "query_template_id": query_template_id,
                                                             "data_item_class_id": data_item_class_id}

                                    data_item_class_action_obj.insert_struct(data_item_action_dict)


                        # Add item

                            # Add item transitions

                            # Add item transitions which have data item actions
