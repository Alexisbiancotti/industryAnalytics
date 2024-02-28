from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task


@dag(
    schedule=None,
    catchup=False,
    tags=["customers", "sql"],
)
def customers():


    @task()
    def createTempTable():
       
       import time
       import logging

       #defining the table name
       tableName =  "customer_" + str(round(time.time()))

       # testing logging in from airflow
       logging.info(f"The temp table name is: {tableName}")

       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       createSQL = f"""
                    create table {tableName} (
                        idCustomer 			int 				PRIMARY KEY,
                        name 				varchar(12) 		NOT NULL,
                        country 			varchar(12) 		CHECK(country in ('Argentina','Brazil','Uruguay'))
                    );
            """

       cursor.execute(createSQL)

       connection.commit()
       cursor.close()
       connection.close()

       return tableName


    @task()
    def getSaveCustomers(tableName: str):
        
        from psycopg2.extras import execute_batch
        import requests
        import json


        response = requests.get(f'http://datasource-dummy:80/customers')
        data = response.json()
        tupleToInsert = [(record, data[record]['name'], data[record]['country']) for record in data]

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        insertSQL = f"""
                INSERT INTO {tableName} (idCustomer, name, country)
                VALUES (%s, %s, %s);
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

       cursor.execute("DELETE FROM customer")

       connection.commit()
       cursor.close()
       connection.close()

    
    @task()
    def insertValues(tableName: str):

       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute(f"""
            INSERT INTO customer
            SELECT * FROM {tableName}
            """)

       connection.commit()
       cursor.close()
       connection.close()


    @task()
    def dropTempTable():
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute(f"""
            INSERT INTO customer
            SELECT * FROM {tableName}
            """)

       connection.commit()
       cursor.close()
       connection.close()
        


    tableName = createTempTable()

    getSaveCustomers(tableName) >> dropTableValues() >> insertValues(tableName) >> dropTempTable()


customers()
