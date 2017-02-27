from db_classes import *
import sqlalchemy as sa
import datetime

class MemoryBoxRunner(object):

    def __init__(self, memory_box, connection, meta_data, data_connections_dict):
        self.memory_box = memory_box
        self.connection = connection
        self.meta_data = meta_data
        self.memory_box_obj = MemoryBoxes(self.connection, self.meta_data)
        memory_box_row_dict = self.memory_box_obj.find_one(self.memory_box)
        self.memory_box_id = memory_box_row_dict.id

        self.data_connection_id = memory_box_row_dict.data_connection_id
        self.data_connection_obj = DataConnections(self.connection, self.meta_data)
        data_connection_row_dict = self.data_connection_obj.find_by_id(self.data_connection_id)
        self.data_connection_name = data_connection_row_dict.name

        self.source_data_connection_uri = data_connections_dict[self.data_connection_name]

        self.source_engine = sa.create_engine(self.source_data_connection_uri)
        self.source_connection = self.source_engine.connect()

    def _odbc_datetime_to_datetime(self, odbc_datetime_string):
        return datetime.datetime.strptime(odbc_datetime_string, "%Y-%m-%d %H:%M")

    def _odbc_date_to_datetime(self, odbc_date_string):
        return datetime.datetime.strptime(odbc_date_string, "%Y-%m-%d")

    def _generate_query_parameters(self, parameters, defaults):
        return {"lower_discharge_date_time": self._odbc_date_to_datetime("2016-09-30"), "upper_discharge_date_time": self._odbc_date_to_datetime("2016-11-01")}

    def run(self, item_class_name):

        transition_state_obj = TransitionStateItemClasses(self.connection, self.meta_data)
        query_template_obj = QueryTemplates(self.connection, self.meta_data)
        item_class_obj = ItemClasses(self.connection, self.meta_data)
        track_item_obj = TrackItems(self.connection, self.meta_data)

        transitions = transition_state_obj.find_transitions_for_memory_box(self.memory_box, item_class_name)

        for transition in transitions:
            query_template_id = transition.query_template_id

            from_state_name = transition.from_state_name
            from_state_id = transition.from_state_id

            to_state_name = transition.to_state_name
            to_state_id = transition.to_state_id

            item_class_id = transition.item_class_id

            action_id = transition.action_id
            action_name = transition.action_name

            if query_template_id is not None:
                query_template_result = query_template_obj.find_by_id(query_template_id)

                parameters = transition.parameters
                default_parameters = transition.defaults

                query_parameters = self._generate_query_parameters(parameters, default_parameters)

                source_cursor = self.source_connection.execute(query_template_result.template, **query_parameters)

                for source_row in source_cursor:

                    track_item_dict = {"item_class_id": item_class_id, "state_id": to_state_id, "transaction_id": source_row.transaction_id}

                    if from_state_id is None:
                        if action_name == "Insert new":
                            track_item_dict["created_at"] = datetime.datetime.utcnow()
                            track_item_dict["updated_at"] = datetime.datetime.utcnow()

                            new_record = track_item_obj.insert_struct(track_item_dict)



