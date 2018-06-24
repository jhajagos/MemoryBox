### For production database to add migrations

import json
import sqlalchemy as sa


def main(config):

    schema = config["schema"]
    connection_uri = config["connection_uri"]

    engine = sa.create_engine(connection_uri)

    connection = engine.connect()
    meta_data = sa.MetaData(connection, schema=schema)
    meta_data.reflect()

    changed_data_items = sa.Table("changed_data_items", meta_data,
                               sa.Column("id", sa.Integer, primary_key=True),
                               sa.Column("initial_data_item_id", sa.ForeignKey("data_items.id")),
                               sa.Column("changed_data_item_id", sa.ForeignKey("data_items.id")),
                               sa.Column("created_at", sa.DateTime))

    meta_data.create_all()

    connection.execute('alter table "%s".data_items add column data_for_diff JSONB' % schema)


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)