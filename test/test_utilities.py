import csv
import sqlalchemy as sa
import datetime


def create_schema_test_database(connection_string, schema_sql):
    eng = sa.create_engine(connection_string)
    connection = eng.connect()

    for sql_statement in schema_sql.split(";"): # Schema must not have any embedded ; in comments
        connection.execute(sql_statement)

    meta_data = sa.MetaData(connection, reflect=True)
    return connection, meta_data


def load_csv_into_database(table_name, csv_file_name, connection, meta_data):
    table_obj = meta_data.tables[table_name]

    date_time_fields_dict = {}
    for column in table_obj.columns:

        if column.type.__class__ == sa.sql.sqltypes.DATETIME:
            date_time_fields_dict[column.name] = 1

    with open(csv_file_name, "rb") as f:
        csv_dict_reader = csv.DictReader(f)

        i = 0
        for row_dict in csv_dict_reader:
            for column in row_dict:
                if column in date_time_fields_dict:
                    if ":" in row_dict[column]:
                        row_dict[column] = datetime.datetime.strptime(row_dict[column], "%Y-%m-%d %H:%M")
                    elif not len(row_dict[column]):
                        row_dict[column] = None
                    else:
                        row_dict[column] = datetime.datetime.strptime(row_dict[column], "%Y-%m-%d")
                elif not len(row_dict[column]):
                    row_dict[column] = None
            connection.execute(table_obj.insert(row_dict))
            i += 1

    return i

