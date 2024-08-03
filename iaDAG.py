from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests

# Function to execute the SELECT statement and return the date
def get_date_from_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgreProd')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT your_date_column FROM your_table LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    # Assuming the date is the first column in the result
    return result[0]

# Function to call the API with the date and return the data
def call_api_with_date(**kwargs):
    ti = kwargs['ti']
    date = ti.xcom_pull(task_ids='get_date_from_postgres')
    response = requests.get(f'your_api_endpoint?date={date}')
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception('API request failed')

# Function to store the data in PostgreSQL
def store_data_in_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='call_api_with_date')
    pg_hook = PostgresHook(postgres_conn_id='postgreProd')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    # Assuming you're inserting JSON data into a table with a single JSONB column
    cursor.execute("INSERT INTO your_target_table (data_column) VALUES (%s)", (json.dumps(data),))
    connection.commit()
    cursor.close()
    connection.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple DAG to execute SQL, call an API, and store data',
    schedule_interval=timedelta(days=1),
)

# Define tasks
get_date = PythonOperator(
    task_id='get_date_from_postgres',
    python_callable=get_date_from_postgres,
    provide_context=True,
    dag=dag,
)

call_api = PythonOperator(
    task_id='call_api_with_date',
    python_callable=call_api_with_date,
    provide_context=True,
    dag=dag,
)

store_data = PythonOperator(
    task_id='store_data_in_postgres',
    python_callable=store_data_in_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
get_date >> call_api >> store_data










/////////////////////////// Version 2 ////////////////////

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG('etl_pipeline', start_date='2023-11-03')

extract_task = PostgresOperator(
    task_id='extract_data',
    sql='SELECT * FROM customers',
    postgres_conn_id='postgres_default',
    dag=dag
)

# load_task = MySqlOperator(
#     task_id='load_data',
#     sql='INSERT INTO customers (id, name, email) VALUES (%(id)s, %(name)s, %(email)s)',
#     mysql_conn_id='mysql_default',
#     params={'extract_task.output': extract_task.output},
#     dag=dag
# )

extract_task >> load_task




///////////// version 3 /////////////////

from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def insert_multiple_json_to_postgres():
    # Example JSON data with multiple records
    json_data = """
    [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"}
    ]
    """
    records = json.loads(json_data)  # Convert JSON string to Python list
    
    # Initialize the PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection_id')
    
    # Prepare list of tuples for the INSERT statement
    tuples_to_insert = [(record['id'], record['name'], record['email']) for record in records]
    
    # SQL INSERT statement
    insert_sql = "INSERT INTO users (id, name, email) VALUES (%s, %s, %s);"
    
    # Get a Postgres connection from the hook
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Use psycopg2.extras.execute_batch for efficient batch inserts
    from psycopg2.extras import execute_batch
    execute_batch(cursor, insert_sql, tuples_to_insert)
    
    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()