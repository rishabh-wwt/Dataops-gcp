from typing import Tuple
from google.cloud import bigquery as bq
from configparser import ConfigParser
from json import loads


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

def create_schema(sch : list) :
    """
    Method to convert table schema str to bigquery table schema format
    Invokes create_col_str_n_nested_schema for resolving nested columns
    """
    schema = []    
    for cols in sch:
        unested_fields , nested_fields = create_col_str_n_nested_schema(cols)    
        unested_col , unested_dtype , unested_mode = unested_fields.split(',')
        schema.append(bq.SchemaField(unested_col , unested_dtype , unested_mode,fields=nested_fields ))
    return schema    
    
def create_col_str_n_nested_schema(nested_sch:dict) -> Tuple :
    '''
        Method converts nested_fields configured for in tables.ini into big query compatible schema
        along with string of un-nested cols
        Note: Currently big query supports nested level upto 15 levels
    '''
    col_str = ''
    if isinstance(nested_sch,dict):

        for key,value in nested_sch.items():        
            nested_col=''
            nested_sch = []
            if key == 'Nested_Fields':
                for nested_fields in value:
                    nested_col , prev_curr_nested_schema = create_col_str_n_nested_schema(nested_fields)
                    nested_coll , nested_dtype , nested_mode = nested_col.split(",")
                    nested_sch.append(bq.SchemaField(nested_coll,nested_dtype,nested_mode,fields=prev_curr_nested_schema))                
            else:
                col_str = col_str + ',' + value           
        return col_str.lstrip(','),nested_sch
    else:
        print("nested_sch is not a dict please pass it as dict")    
    

def create_bq_table(table_id, table_schema_str : list):
    """
    Creates new bq table as per the schema defined in tables.ini
    """
    if isinstance(table_schema_str,list):

        table_ref = bq.Table(table_ref=table_id, schema=create_schema(table_schema_str))
        table = client.create_table(table_ref)
        table_info = {
            "table_type": "new",
            "table_creation": table.created,
            "table_Schema": table.schema,
        }
        return table_info
    else:
        print("Pass correct schema type it should be a list")        

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
    table_schema = loads(config_table.get(table_name, "schema_string"))
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
    table_name = "sdk_3_lvl_nested_table"
    table_id, table_schema_str = get_table_schema_name(table_name=table_name)
    table_schema_str = table_schema_str["Fields"]

    try:

        table_info = check_n_update_bq_table_schema(
            client=client, table_id=table_id, table_schema_str=table_schema_str
        )

    except:

        table_info = create_bq_table(table_id, table_schema_str)

    print(table_info)