

This guide will describes how to configure and run MemoryBox. MemoryBox implements
a state-machine for tracking items in a database. It can update and track items
in the database as items are changed. Before you can run a MemoryBox to
track items in an external database two configuration files need to be set. The first is a runtime.json 
and the second is a JSON file.

The runtime JSON is simple configuration file consisting of keys in a dictionary.

```json
{
    "connection_uri": "postgresql+psycopg2://postgres:redacted@localhost/postgres",
    "db_schema": "memorybox",
    "root_file_path": "./test/",
    "data_connections":
        {"ehr_warehouse": "sqlite:///./files/test_encounter.db3"}
}
```
The `"connection_uri"` specifies the SQLAlchemy database. The connection URI must
specify a PostGreSQL database of version 9.6 and use the psycopg2 driver for Python.
Multiple named memory_boxes can exists in a single schema.  The tables are stored in `"db_schema"` key.  The schema must be created and the PostGreSQL
user must have full access to the schema. The key `"root_file_path"` does ?. A `"data_connections"`
specify the connection string for external databases. 
The `"data_connections"` key points to a dictionary with keys pointing to SQLAlchemy connection string.
The external database does not have to be a PostGreSQL database. The example here is to a SQLite embedded database.

The driver behind a MemoryBox is the memory_box.json file which specifies how items
are tracked and updated during the items life cycle.


```json
{
  "data_connection": 
  {
      "name": "ehr_warehouse",
      "data_connection_type": "SQLite"
  },
  "name": "encounter",
  "items": {
    "classes": [
      {"name": "discharges",
       "transitions":
        [
          {
            "from": null,
            "to": "New",
            "query_template": "discharge_query",
            "action": "Insert new",
            "parameters": {"lower_discharge_date_time": "2016-10-01", "upper_discharge_date_time": "2016-12-01"},
            "defaults": {},
            "actions": [
              {"name": "Insert",
               "data_item_class": "encounter_detail"
              }
            ]
          }
        ],
        "data_items": {
          "classes": [
            {"name": "encounter_detail",
              "data_type": "JSON",
              "actions": [
                {
                  "name": "Insert",
                  "query_template": "encounter_detail_query",
                  "parameters": {"encounter_number": "_transaction_id"}
                }
              ]
            },
            {"name": "encounter_dx",
              "data_type": "JSON",
              "actions": [
                {
                  "name": "Insert",
                  "query_template": "encounter_dx_query",
                  "parameters":  {"encounter_number": "_transaction_id"}
                }
              ]
            }
          ]
        }
      }
    ]
  },
  "query_templates": [
    {
      "name": "discharge_query",
      "template": "select encounter_number as transaction_id from encounters where discharge_date_time is not null and discharge_date_time >= :lower_discharge_date_time and discharge_date_time < :upper_discharge_date_time",
      "parameter_list": ["lower_discharge_date_time", "upper_discharge_date_time"]
    }
  ]
}

```