from db_classes import *


class MemoryBoxRunner(object):

    def __init__(self, memory_box, connection, meta_data):
        self.memory_box = memory_box
        self.connection = connection
        self.meta_data = meta_data
        self.memory_box_obj = MemoryBoxes(self.connection, self.meta_data)
        self.memory_box_id = self.memory_box_obj.find_one(self.memory_box).id

    def run(self):
        pass