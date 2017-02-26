from db_classes import *
import sqlalchemy as sa

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

    def run(self):

        transition_state_obj = TransitionStateItemClasses(self.connection, self.meta_data)
        transitions = transition_state_obj.find_transitions_for_memory_box(self.memory_box)
        #print(transitions)
