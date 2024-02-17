from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task



@dag(
    schedule=None,
    catchup=False,
    tags=["items", "sql"],
)
def items():


    @task()
    def dropTableValues():
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute("DELETE FROM item")

       cursor.close()
       connection.close()
       return 


    @task()
    def getSaveItems():
        
        from psycopg2.extras import execute_batch
        import requests
        import json

        response = requests.get(f'http://datasource-dummy:80/items')
        data = response.json()
        tupleToInsert = [(data[record]['name'], data[record]['price'], data[record]['family'], data[record]['cicleTime']) for record in data]

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insertSQL = """
                INSERT INTO item (name, price, family, cicleTime)
                VALUES (%s, %s, %s, %s);
                """
        
        execute_batch(cursor, insertSQL, tupleToInsert)
        
        connection.commit()
        cursor.close()
        connection.close()

    dropTableValues()

    getSaveItems()

items()
