from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

import requests
import datetime
import json



@dag(
    catchup=False,
    tags=["sql","quota"],
    start_date=datetime.datetime(2024, 2, 24),
    #comenting the schedule in order to use the dummy data for the dashboard
    #schedule_interval= "*/5 * * * *",
)
def qouta():

    @task()
    def getQuota():

        from psycopg2.extras import execute_batch   

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
                        SELECT DISTINCT 
                            DATE(DATE_TRUNC('month', T1.createddate)) as period,
                            T1.idItem
                        FROM analyticsdata.salesorderAirflow T1
                        LEFT JOIN analyticsdata.quotaAirflow T2
                        ON DATE(DATE_TRUNC('month', T1.createddate)) = T2.Period
                            AND
                        T1.idItem = T2.idItem
                        WHERE T2.idItem IS NULL;
                       """)
        
        # this returns a nested list with the results 
        result = cursor.fetchall()

        for i in range(len(result)):
            
            response = requests.get(f'http://datasource-dummy:80/quota?fromDate={result[i][0]}&idItem={result[i][1]}')
            data = response.json()
        
            sotupleToInsert = tuple(data.values())        
            
            insertSQL = """
                    INSERT INTO analyticsdata.quotaAirflow (period, idItem, quota)
                    VALUES (%s, %s, %s);
                    """
            cursor.execute(insertSQL, sotupleToInsert)

            connection.commit()

        cursor.close()
        connection.close()
    
    getQuota()

qouta = qouta()