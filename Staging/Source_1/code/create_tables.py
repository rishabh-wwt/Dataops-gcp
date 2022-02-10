from importlib import import_module
import importlib
from google.cloud import bigquery as bq
from Staging import project_datest
# import importlib

# staging = importlib.import_module('Staging')
# print(staging.__name__)

def _get_client():
    bq_client = bq.Client()
    return bq_client

def _construct_project_dataset_id(table_id,project,dataset):
    return project.strip() + '.' + dataset.strip() + '.' + table_id.strip()


def create_schema(sch):
    '''
        Method to convert table schema str to bigquery table schema format
    
    '''
    schema : list = []
    nested_schema : list =[]
    all_schema = sch.split(",")
    for cols_info in all_schema:
        col,dtype,mode = cols_info.split(":")

        if dtype == 'RECORD':
            nested_fields = mode.split("->")                        
            for nested_cols_info in nested_fields[1:]:
                nested_col , nested_dtype , nested_mode = nested_cols_info.split("-")
                nested_schema.append(bq.SchemaField(nested_col.strip(),nested_dtype,mode=nested_mode.strip()))
            schema.append(bq.SchemaField(col.strip(),dtype,mode=nested_fields[0].strip(),fields=nested_schema))            
        else:
            schema.append(bq.SchemaField(col.strip(),dtype,mode=mode.strip()))

    return schema

def update_table_schema(client,tab, sch):
    ''' 
        Method to update the table schema
        Limitations observerd : Schema updation failing if new fields are added in REQUIRED mode. It works with mode as NULLABLE
        Error Message : Provided Schema does not match Table gcp-bajpaid:dev.sdk_table. Cannot add required fields to an existing schema. (field: Id)
    '''
    tab.schema = sch
    upd_tab = client.update_table(tab,fields=["schema"])
    table_info = {"table_type":"existing","table_name":upd_tab.table_id,"old_schema":'',"new_Schema":upd_tab.schema}
    return table_info

if __name__ == '__main__':

    client = _get_client()
    table_name = "sdk_table"
    from Source_1.table import sdk_table
    
    schema_string = table_name.schema_string
    table_id = _construct_project_dataset_id(table_name,project_datest.project,project_datest.dataset)
    try:
        table = client.get_table(table_id)    
        table_info = {"table_type":"existing","table_creation" :table.created,"table_Schema":table.schema, "table_etag": table.etag,
        "table_path":table.path}
        table_schema = create_schema(schema_string)
        if table.schema != table_schema:
            print ("Schema update detected")
            table_info = update_table_schema(client,table,table_schema)
            table_info['old_schema'] = table.schema
    except:
        table_ref = bq.Table(table_ref=table_id,schema=create_schema(schema_string))
        table = client.create_table(table_ref)
        table_info = {"table_type":"new","table_creation" :table.created,"table_Schema":table.schema} 

    print(table_info)