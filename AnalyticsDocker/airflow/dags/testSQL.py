from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

import requests
import datetime
import json

@dag(
    schedule=None,
    catchup=False,
    tags=["testSQL"],
)
def testSQL():


    # @task()
    # def getLastDate():

    #     # getting the data from sql
    #     pg_hook = PostgresHook(postgres_conn_id='postgreProd')
    #     connection = pg_hook.get_conn()
    #     cursor = connection.cursor()
    #     cursor.execute("SELECT max(createdDate) FROM salesOrder")
    #     result = cursor.fetchone()
    #     lastDate = result[0]
    #     cursor.close()
    #     connection.close()

    #     if lastDate == None:

    #         lastDate = datetime.datetime(2024, 1, 1).strftime("%Y-%m-%d")

    #     return lastDate
    
    # @task()
    # def getSaveSO(lastDate):
        
    #     response = requests.get(f'http://datasource-dummy:80/soData?paramDate={lastDate}')

    #     data = response.json()

    #     pg_hook = PostgresHook(postgres_conn_id='postgreProd')

    #     insert_sql = """
    #             INSERT INTO salesorder (idCustomer, idItem, createdDate, dueDate, shipDate, qty, qtyFullfilled, qtyShipped, soStatus)
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    #             """
        
    #     pg_hook.run(insert_sql, parameters=(data['idCustomer'], 
    #                                         data['idItem'], 
    #                                         data['createdDate'],
    #                                         data['dueDate'], 
    #                                         data['shipDate'], 
    #                                         data['qty'],
    #                                         data['qtyFullfilled'], 
    #                                         data['qtyShipped'], 
    #                                         data['soStatus']))

    @task()
    def getSaveItems():
        
        from psycopg2.extras import execute_batch

        response = requests.get(f'http://datasource-dummy:80/items')

        data = response.json()

        tuples_to_insert = [(data[record]['name'], data[record]['price'], data[record]['family'], data[record]['cicleTime']) for record in data]

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insert_sql = """
                INSERT INTO item (name, price, family, cicleTime)
                VALUES (%s, %s, %s, %s);
                """
        
        execute_batch(cursor, insert_sql, tuples_to_insert)
        
        connection.commit()
        cursor.close()
        connection.close()

    getSaveItems()

    # getSaveSO(lastDate)

testSQL()
