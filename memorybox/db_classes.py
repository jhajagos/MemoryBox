class DBClass(object):
    """Base Class for a PostgreSQL table in a schema"""
    def __init__(self, connection, meta_data):
        self.connection = connection
        self.meta_data = meta_data
        self.schema = meta_data.schema
        self.table_name = self._table_name()

        if self.schema is not None:
            self.table_name_with_schema = self.schema + "." + self.table_name
        else:
            self.table_name_with_schema = self.table_name

        self.table_obj = self.meta_data.tables[self.table_name_with_schema]

    def _table_name(self):
        return ""

    def insert_struct(self, data_struct):
        return self.connection.execute(self.table_obj.insert(data_struct).returning(self.table_obj.c.id)).fetchone()[0]

    def update_struct(self, row_id, update_dict):
        sql_expr = self.table_obj.update().where(self.table_obj.c.id == row_id).values(update_dict)
        self.connection.execute(sql_expr)

    def find_by_id(self, row_id):
        sql_expr = self.table_obj.select().where(self.table_obj.c.id == row_id)
        connection = self.connection.execute(sql_expr)
        return list(connection)[0]


class DBClassName(DBClass):
    """Base class for working with table name"""

    def __init__(self, name, connection, meta_data, create_if_does_not_exists=True):

        self.connection = connection
        self.meta_data = meta_data
        self.schema = meta_data.schema
        self.table_name = self._table_name()
        self.name = name

        if self.schema is not None:
            self.table_name_with_schema = self.schema + "." + self.table_name
        else:
            self.table_name_with_schema = self.table_name

        self.table_obj = self.meta_data.tables[self.table_name_with_schema]

        self.name_obj = self._find_by_name(self.name)
        if self.name_obj is None and create_if_does_not_exists:
            self._insert_name(self.name)
            self.name_obj = self._find_by_name(self.name)

    def get_id(self):
        return self.name_obj.id

    def _find_by_name(self, name):
        find_expr = self.table_obj.select().where(self.table_obj.columns["name"] == name)
        cursor = self.connection.execute(find_expr)
        cursor_result = list(cursor)
        if len(cursor_result):
            return cursor_result[0]
        else:
            return None

    def _insert_name(self, name):
        self.connection.execute(self.table_obj.insert({"name": name}))


class Actions(DBClass):
    def _table_name(self):
        return "actions"


class States(DBClass):
    def _table_name(self):
        return "states"


class DataConnectionTypes(DBClass):
    def _table_name(self):
        return "data_connection_types"


class DataConnections(DBClass):
    def _table_name(self):
        return "data_connections"


class ItemClasses(DBClass):
    def _table_name(self):
        return "item_classes"


class MemoryBoxes(DBClass):
    def _table_name(self):
        return "memory_boxes"


class DataItems(DBClass):
    def _table_name(self):
        return "data_items"


class DataItemTypes(DBClass):
    def _table_name(self):
        return "data_item_types"


class DataItemActionTransitionStateItems(DBClass):
    def _table_name(self):
        return "data_item_action_transition_state_items"


class DataItemClassActions(DBClass):
    def _table_name(self):
        return "data_item_class_actions"


class TransitionStateItemClasses(DBClass):
    def _table_name(self):
        return "transition_state_item_classes"


class TrackItems(DBClass):
    def _table_name(self):
        return "track_items"


class TrackItemUpdates(DBClass):
    def _table_name(self):
        return "track_item_updates"


class QueryTemplates(DBClass):
    def _table_name(self):
        return "query_templates"