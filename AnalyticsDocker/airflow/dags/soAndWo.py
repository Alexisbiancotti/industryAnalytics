from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

import requests

import json

@dag(
    schedule=None,
    catchup=False,
    tags=["testSQL"],
)
def soAndWO():


    @task()
    def getLastDate():

        import datetime

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT max(createdDate) FROM salesOrder")
        result = cursor.fetchone()
        lastDate = result[0]

        cursor.close()
        connection.close()

        if lastDate == None:

            lastDate = datetime.datetime(2024, 1, 1).strftime("%Y-%m-%d")

        return lastDate
    
    
    @task()
    def getSaveSOAndWO(lastDate):

        from psycopg2.extras import execute_batch   
        
        response = requests.get(f'http://datasource-dummy:80/soData?paramDate={lastDate}')
        data = response.json()        
        tupleToInsert = tuple(data.values())

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insertSQL = """
                INSERT INTO salesorder (idCustomer, idItem, createdDate, dueDate, shipDate, qty, qtyFullfilled, qtyShipped, soStatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        cursor.execute(insertSQL, tupleToInsert)

        # execute_batch(cursor, insertSQL, tupleToInsert)
        
        connection.commit()
        cursor.close()
        connection.close()

        return tupleToInsert

    # @task()
    # def getSaveItems():
        
    #     from psycopg2.extras import execute_batch

    #     response = requests.get(f'http://datasource-dummy:80/items')

    #     data = response.json()

    #     tuples_to_insert = [(data[record]['name'], data[record]['price'], data[record]['family'], data[record]['cicleTime']) for record in data]

    #     pg_hook = PostgresHook(postgres_conn_id='postgreProd')
    #     connection = pg_hook.get_conn()
    #     cursor = connection.cursor()
        
    #     insert_sql = """
    #             INSERT INTO item (name, price, family, cicleTime)
    #             VALUES (%s, %s, %s, %s);
    #             """
        
    #     execute_batch(cursor, insert_sql, tuples_to_insert)
        
    #     connection.commit()
    #     cursor.close()
    #     connection.close()

    lastDate = getLastDate()

    getSaveSOAndWO(lastDate)

soAndWO()
