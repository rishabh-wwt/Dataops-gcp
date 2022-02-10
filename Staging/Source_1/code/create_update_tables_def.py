from google.cloud import bigquery as bq
from configparser import ConfigParser


def _get_client():
    bq_client = bq.Client()
    return bq_client


def _construct_project_dataset_id():

    config = ConfigParser()
    config.read("Staging/project.ini")
    project = config.get("project_id", "project")
    dataset = config.get("dataset_id", "dataset")
    base_path = config.get("base_path", "src_1_base_path")

    return project.strip() + "." + dataset.strip(), base_path

def create_schema(sch):
    """
    Method to convert table schema str to bigquery table schema format
    Note : Supports only nesting only upto level 1

    """
    schema: list = []
    nested_schema: list = []
    all_schema = sch.split(",")
    for cols_info in all_schema:
        col, dtype, mode = cols_info.split(":")

        if dtype == "RECORD":
            nested_fields = mode.split("->")
            for nested_cols_info in nested_fields[1:]:
                nested_col, nested_dtype, nested_mode = nested_cols_info.split("-")
                nested_schema.append(
                    bq.SchemaField(
                        nested_col.strip(), nested_dtype, mode=nested_mode.strip()
                    )
                )
            schema.append(
                bq.SchemaField(
                    col.strip(),
                    dtype,
                    mode=nested_fields[0].strip(),
                    fields=nested_schema,
                )
            )
        else:
            schema.append(bq.SchemaField(col.strip(), dtype, mode=mode.strip()))

    return schema

def create_bq_table(table_id, table_schema_str):
    """
    Creates new bq table as per the schema defined in tables.ini
    """
    table_ref = bq.Table(table_ref=table_id, schema=create_schema(table_schema_str))
    table = client.create_table(table_ref)
    table_info = {
        "table_type": "new",
        "table_creation": table.created,
        "table_Schema": table.schema,
    }
    return table_info

def get_bq_table_details(client, table_id):
    table = client.get_table(table_id)
    table_info = {
        "table_type": "existing",
        "table_creation": table.created,
        "table_Schema": table.schema,
        "table_etag": table.etag,
        "table_path": table.path,
    }
    return table, table_info

def update_table_schema(client, tab, sch):
    """
    Method to update the table schema
    Limitations observerd : Schema updation failing if new fields are added in REQUIRED mode. It works with mode as NULLABLE
    ``Error Message : Provided Schema does not match Table gcp-bajpaid:dev.sdk_table. Cannot add required fields to an existing schema. (field: Id)``
    """
    tab.schema = sch
    upd_tab = client.update_table(tab, fields=["schema"])
    table_info = {
        "table_type": "existing",
        "table_name": upd_tab.table_id,
        "old_schema": "",
        "new_Schema": upd_tab.schema,
    }
    return table_info

def get_table_schema_name(table_name):
    """
    Get table schema from the configuration file

    """
    project_datest, src_base_path = _construct_project_dataset_id()
    table_config_path = src_base_path + "/table/tables.ini"
    fully_qualified_table_id = project_datest + "." + table_name
    config_table = ConfigParser()
    config_table.read(table_config_path)
    table_schema = config_table.get(table_name, "schema_string")

    return fully_qualified_table_id, table_schema

def check_n_update_bq_table_schema(client, table_id, table_schema_str):
    """
    Detects for schema change and then add_new cols in the existing table
    """

    table, table_info = get_bq_table_details(client, table_id)

    old_table_schema = table.schema
    new_table_schema = create_schema(table_schema_str)

    if old_table_schema != new_table_schema:
        print("Schema update detected")
        table_info = update_table_schema(client, table, new_table_schema)
        table_info["old_schema"] = old_table_schema

    return table_info

if __name__ == "__main__":

    client = _get_client()
    table_name = "sdk_table"
    table_id, table_schema_str = get_table_schema_name(table_name=table_name)

    try:

        table_info = check_n_update_bq_table_schema(
            client=client, table_id=table_id, table_schema_str=table_schema_str
        )

    except:

        table_info = create_bq_table(table_id, table_schema_str)

    print(table_info)