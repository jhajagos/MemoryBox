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
        cursor = self.connection.execute(sql_expr)
        return list(cursor)[0]

    def find_one(self, value_string, field_name="name"):
        sql_expr = self.table_obj.select().where(self.table_obj.c[field_name] == value_string)
        cursor = self.connection.execute(sql_expr)

        result_list = list(cursor)
        if len(result_list):
            return result_list[0]
        else:
            return None

    def find_by_sql(self, sql_query):
        return list(self.connection.execute(sql_query))


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

class DataItemClasses(DBClass):
    def _table_name(self):
        return "data_item_classes"

class DataItemTypes(DBClass):
    def _table_name(self):
        return "data_item_types"


class DataItemActionsTransitionStateItems(DBClass):
    def _table_name(self):
        return "data_item_actions_transition_state_items"


class DataItemClassActions(DBClass):
    def _table_name(self):
        return "data_item_class_actions"


class TransitionStateItemClasses(DBClass):
    def _table_name(self):
        return "transition_state_item_classes"

    def find_transitions_for_memory_box(self, memory_box_name):

        query_dict = {"schema": self.meta_data.schema, "memory_box_name": memory_box_name}

        sql_query = """
  select ic.*,
       s1.name as from_state_name, s2.name as to_state_name,
       a.name as action_name,
       i.name as item_class_name
  from %(schema)s.transition_state_item_classes ic
  left outer join %(schema)s.states s1 on s1.id = ic.from_state_id
  join %(schema)s.states s2 on s2.id = ic.to_state_id
  join %(schema)s.actions a on a.id = ic.action_id
  join %(schema)s.item_classes i on i.id = ic.item_class_id
  join %(schema)s.memory_boxes mb on i.memory_box_id = mb.id
  where mb.name = '%(memory_box_name)s'
  order by ic.id
        """ % query_dict

        return self.find_by_sql(sql_query)


class TrackItems(DBClass):
    def _table_name(self):
        return "track_items"


class TrackItemUpdates(DBClass):
    def _table_name(self):
        return "track_item_updates"


class QueryTemplates(DBClass):
    def _table_name(self):
        return "query_templates"