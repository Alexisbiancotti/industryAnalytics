from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

import requests
import datetime
import json


# the sensors runs and waits until the external dag finishes, so it needs to run in the same time o explicite the difference
# if the external dag doesnt run or something else happens it could make the dag to run forever
@dag(
    catchup=False,
    tags=["testSQL"],
    start_date=datetime.datetime(2024, 2, 24),
    #comenting the schedule in order to use the dummy data for the dashboard
    schedule_interval= "*/2 * * * *",
)
def wo():

    soSuccess = ExternalTaskSensor(
        task_id= 'soSuccess',
        external_dag_id='so',
        external_task_id=None, # Set to None to wait for the entire DAG
        timeout=60,
        # execution_delta=timedelta(hours=1), # Time difference with the first DAG's execution
    )

    @task()
    def getWO():

        from psycopg2.extras import execute_batch   

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
                       select  
                            T1.idso,
                            T1.iditem,
                            T1.createddate, 
                            T1.qtyfullfilled, 
                            T1.sostatus
                        from analyticsdata.salesOrderAirflow T1
                        left join analyticsdata.workOrderAirflow T2
                        on T1.idso = T2.idso 
                        where 
                            T1.sostatus <> 'Approved'
                            and 
                            T2.idwo is null
                       """)
        
        # this returns a nested list with the results 
        result = cursor.fetchall()

        for i in range(len(result)):
        
            response = requests.get(f'http://datasource-dummy:80/woData?idSO={result[i][0]}&idItem={result[i][1]}&fromDate={result[i][2]}&qtyFullfilled={result[i][3]}&soStatus={result[i][4]}')
            data = response.json()        
            sotupleToInsert = tuple(data.values())        
            
            insertSQL = """
                    INSERT INTO analyticsdata.workOrderAirflow (idSO, idItem, createdDate, closedDate, qtyCreated, scrapQty)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """
            cursor.execute(insertSQL, sotupleToInsert)

            connection.commit()

        cursor.close()
        connection.close()
    
    soSuccess >> getWO()

test = wo()