from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task


@dag(
    schedule=None,
    catchup=False,
    tags=["customers", "sql"],
)
def customers():


    @task()
    def dropTableValues():
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute("DELETE FROM customer")

       cursor.close()
       connection.close()
       return 


    @task()
    def getSaveCustomers():
        
        from psycopg2.extras import execute_batch
        import requests
        import json


        response = requests.get(f'http://datasource-dummy:80/customers')
        data = response.json()
        tupleToInsert = [(data[record]['name'], data[record]['country']) for record in data]

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insertSQL = """
                INSERT INTO customer (name, country)
                VALUES (%s, %s);
                """
        
        execute_batch(cursor, insertSQL, tupleToInsert)
        
        connection.commit()
        cursor.close()
        connection.close()

    dropTableValues()

    getSaveCustomers()

customers()
