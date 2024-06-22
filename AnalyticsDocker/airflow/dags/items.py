from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task


@dag(
    schedule=None,
    catchup=False,
    tags=["items", "sql"],
)
def items():


    @task()
    def createTempTable():
       
       import time
       import logging

       #defining the table name
       tableName =  "item_" + str(round(time.time()))

       # testing logging in from airflow
       logging.info(f"The temp table name is: {tableName}")

       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       createSQL = f"""
                    create table {tableName} (
                        idItem 				int 				PRIMARY KEY,
                        name 				varchar(12) 		CHECK(name in ('Sifon Simple','Sifon PVC','Sifon Doble')),
                        price	 			float(2) 			CHECK(price > 0),
                        family 				varchar(12) 		CHECK(family in ('Family A','Family B')),
                        cicleTime	 		float(2) 			CHECK(cicleTime > 0),
                        cicleDev	 		float(2) 			CHECK(cicleDev > 0)
                    );
            """

       cursor.execute(createSQL)

       connection.commit()
       cursor.close()
       connection.close()

       return tableName


    @task()
    def getSaveItems(tableName: str):
        
        from psycopg2.extras import execute_batch
        import requests
        import json


        response = requests.get(f'http://datasource-dummy:80/items')
        data = response.json()
        tupleToInsert = [(record, 
                          data[record]['name'],
                          data[record]['price'],
                          data[record]['family'],
                          data[record]['cicleTime'],
                          data[record]['cicleDev']) for record in data]
        
        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insertSQL = f"""
                INSERT INTO {tableName} (idItem, name, price, family, cicleTime, cicleDev)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
        
        execute_batch(cursor, insertSQL, tupleToInsert)
        
        connection.commit()
        cursor.close()
        connection.close()



    @task()
    def dropTableValues():
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute("DELETE FROM item")

       connection.commit()
       cursor.close()
       connection.close()

    
    @task()
    def insertValues(tableName: str):

       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute(f"""
            INSERT INTO item
            SELECT * FROM {tableName};
            """)

       connection.commit()
       cursor.close()
       connection.close()


    @task()
    def dropTempTable(tableName : str):
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute(f"""
            DROP TABLE IF EXISTS {tableName}; 
            """)

       connection.commit()
       cursor.close()
       connection.close()
        


    tableName = createTempTable()

    getSaveItems(tableName) >> dropTableValues() >> insertValues(tableName) >> dropTempTable(tableName)


items()
