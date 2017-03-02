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

    def _get_transitions_data_item_classes(self, transition_state_item_class_id):

        sql_query_dict = {"schema": self.meta_data.schema, "transition_state_item_class_id": transition_state_item_class_id}

        sql_query = """
        select dia.transition_state_item_class_id, dica.*, dic.name as data_item_class_name, a.name as action_name,
          q.template as query_template, dit.name as data_type_name, dit.id as data_item_type_id
         from
  %(schema)s.data_item_actions_transition_state_items dia
  join %(schema)s.transition_state_item_classes tsic on tsic.id = dia.transition_state_item_class_id
  join %(schema)s.data_item_class_actions dica ON dica.id = dia.data_item_class_action_id
  join %(schema)s.data_item_classes dic on dic.id = dica.data_item_class_id
  join %(schema)s.data_item_types dit on dit.id = dic.data_item_type_id
  join %(schema)s.actions a on a.id = dica.action_id
  left outer join %(schema)s.query_templates q on q.id = dica.query_template_id
  where dia.transition_state_item_class_id = %(transition_state_item_class_id)s
        """ % sql_query_dict

        return list(self.connection.execute(sql_query))

    def _generate_query_parameters(self, parameters, defaults):
        """Generates from passed in parameters and default parameter values for query template"""
        #TODO: This is hard coded for testing purposes
        return {"lower_discharge_date_time": self._odbc_date_to_datetime("2016-09-30"), "upper_discharge_date_time": self._odbc_date_to_datetime("2016-11-01")}

    def _update_data_items(self, track_item_id, state_id, data_item_actions):
        track_item_update_obj = TrackItemUpdates(self.connection, self.meta_data)
        data_item_obj = DataItems(self.connection, self.meta_data)
        track_item_update_struct = {"track_item_id": track_item_id, "state_id": state_id, "created_at": datetime.datetime.utcnow()}
        track_item_update_id = track_item_update_obj.insert_struct(track_item_update_struct)

        for data_item_action in data_item_actions:

            query_parameters = data_item_action.parameters
            query_template = data_item_action.query_template

            data_item_class_name = data_item_action.data_item_class_name
            data_item_class_id = data_item_action.data_item_class_id
            data_item_type_id = data_item_action.data_item_type_id
            cursor = self.source_connection.execute(query_template, **query_parameters)

            data_item_dict = {"data_item_class_id": data_item_class_id,
                              "data_item_type_id": data_item_type_id, "track_item_update_id": track_item_update_id,
                              "created_at": datetime.datetime.utcnow()
                              }

            if data_item_class_name == "JSON":
                data = []
                for row in cursor:
                    data += [row]
                data_item_dict["data"]

            #TODO: Add support for other types

            data_item_obj.insert_struct(data_item_dict)





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

            transition_state_item_class_id = transition.id
            data_item_transitions_to_process = self._get_transitions_data_item_classes(transition_state_item_class_id)

            parameters = transition.parameters
            default_parameters = transition.defaults

            if query_template_id is not None:
                query_template_result = query_template_obj.find_by_id(query_template_id)
                query_parameters = self._generate_query_parameters(parameters, default_parameters)
            else:
                query_parameters = None

            if from_state_id is None: # Bringing new items to track into the database

                source_cursor = self.source_connection.execute(query_template_result.template, **query_parameters)
                for source_row in source_cursor:

                    track_item_dict = {"item_class_id": item_class_id, "state_id": to_state_id, "transaction_id": source_row.transaction_id}

                    if action_name == "Insert new":
                        track_item_dict["updated_at"] = datetime.datetime.utcnow()
                        track_item_dict["created_at"] = datetime.datetime.utcnow()
                        if from_state_id is None:

                            track_item_result = track_item_obj.find_by_transaction_id(track_item_dict["transaction_id"], item_class_id)

                            if not len(track_item_result):
                                track_item_id = track_item_obj.insert_struct(track_item_dict)
                                self._update_data_items(track_item_id, to_state_id, data_item_transitions_to_process)
                                # TODO: trigger associated data item updates

            else: # Handle other transitions by trigger or time elapsed / age out

                if action_name == "Update":
                    transaction_id_dict = {}
                    if query_template_result is not None:
                        source_cursor = self.source_connection.execute(query_template_result.template, **query_parameters)
                        for row in source_cursor:
                            transaction_id_dict[row.transaction_id] = 1
                else:
                    transaction_id_dict = None

                cursor = track_item_obj.find_by_from_state_id(from_state_id, item_class_id)

                for row in cursor:
                    transaction_id = row.transaction_id
                    data_item_updates = 0
                    state_item_update = 0

                    if transaction_id_dict is not None:
                        if transaction_id in transaction_id_dict:
                            data_item_updates = 1
                            state_item_update = 1
                    else:
                        pass

                    if state_item_update:
                        pass
                        # TODO: update state

                    if data_item_updates:
                        pass
                        # TODO: trigger associated with data item update






