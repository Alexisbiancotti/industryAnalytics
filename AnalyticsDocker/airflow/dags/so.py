from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

import requests
import datetime
import json

@dag(
    schedule=None,
    catchup=False,
    start_date=datetime.datetime(2024, 2, 24),
    schedule_interval= "* * * * *",
    tags=["testSQL"],
)
def so():


    @task()
    def getLastDate():

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
    def getSaveSO(lastDate):

        from psycopg2.extras import execute_batch   
        
        response = requests.get(f'http://datasource-dummy:80/soData?paramDate={lastDate}')
        data = response.json()        
        sotupleToInsert = tuple(data.values())

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        
        insertSQL = """
                INSERT INTO salesorder (idCustomer, idItem, createdDate, dueDate, shipDate, qty, qtyFullfilled, qtyShipped, soStatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        cursor.execute(insertSQL, sotupleToInsert)
        
        connection.commit()
        cursor.close()
        connection.close()

    lastDate = getLastDate()

    getSaveSO(lastDate)

so()
