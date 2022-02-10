from airflow import models
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from tenacity import retry

default_args = {'owner': 'divyaansh',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 24),
    'email': ['divyaansh.bajpai@wwt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0}


def check_n_create_table():
    """ Checks and creates table if its not present"""

    check_table_exists = BigQueryTableExistenceSensor(task_id="check_table_exists", \
                                                      project_id="gcp-bajpaid", \
                                                      dataset_id="dev", \
                                                      table_id="orders")
    
    if not check_table_exists:
        print("table present" , check_table_exists.project_id , check_table_exists.table_id)
    else:
        create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    project_id="gcp-bajpaid",
    dataset_id="dev",
    table_id="airflow_table",
    schema_fields=[
        {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)

with models.DAG(dag_id='bigquery_pythonop',
                default_args=default_args,
                    schedule_interval='@once',
                        catchup=False) as dag:
                         
    # check_and_create_table = PythonOperator(task_id='check_and_create_in_bigquery',
    # python_callable= check_n_create_table,dag=dag)

    check_table_exists = BigQueryTableExistenceSensor(task_id="check_table_exists", \
                                                      project_id="gcp-bajpaid", \
                                                      dataset_id="dev", \
                                                      table_id="orders")

    check_airflow_table_exists = BigQueryTableExistenceSensor(task_id="check_airflow_table_exists", \
                                                      project_id="gcp-bajpaid", \
                                                      dataset_id="dev", \
                                                      table_id="airflow_table")

    create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    project_id="gcp-bajpaid",
    dataset_id="dev",
    table_id="airflow_table",
    schema_fields=[
        {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
                  ]
    )

[check_table_exists,check_airflow_table_exists] >> create_table