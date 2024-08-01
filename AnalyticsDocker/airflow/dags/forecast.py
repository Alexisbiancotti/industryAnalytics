from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

import requests
import datetime
import json

import pandas as pd
from prophet import Prophet



@dag(
    catchup=False,
    tags=["sql","forecast"],
    #start_date=datetime.datetime(2024, 2, 24),
    #comenting the schedule in order to use the dummy data for the dashboard
    #schedule_interval= "*/5 * * * *",
)
def forecast():

    @task()
    def createTempTable():
       
        import time
        import logging

        #defining the table name
        tableName =  "forecast_" + str(round(time.time()))

        # testing logging in from airflow
        logging.info(f"The temp table name is: {tableName}")

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        createSQL = f"""
                        create table analyticsdata.{tableName} (
                            forecastDate    		date			NOT NULL,
                            forecastedValue 		float(2) 		,
                            lowerLimit 				float(2) 		,
                            upperLimit	 			float(2)     	,
                            realValue 				float(2)		CHECK(realValue > 0),
                            itemID	 				int				NOT NULL	
                            );
                """

        cursor.execute(createSQL)

        connection.commit()
        cursor.close()
        connection.close()

        return tableName

    @task()
    def calculateForecast(tableName: str):

        from psycopg2.extras import execute_batch  
        from datetime import timedelta
        import random 
        import numpy as np

        pg_hook = PostgresHook(postgres_conn_id='postgreProd')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
                        SELECT 
                            iditem           AS Item,
                            socreateddate    AS Date,
                            sum(saleprice)   AS Sales
                        FROM analyticsdata.stg_consolidated
                        GROUP BY iditem, socreateddate;
                       """)
        
        # this returns a nested list with the results 
        result = cursor.fetchall()

        # converting the nested list to a pandas DF in order to be used with prophet
        result = pd.DataFrame(result, columns=['item', 'date', 'sales'])

        items = result['item'].unique()

    
        for item in items:
            
            # Creating a dataframe with only one item and the names required by prophet
            df_item = result[result['item'] == item][['date', 'sales']].rename(columns={'date': 'ds', 'sales': 'y'})

            # Creating the model and making the predictions
            model = Prophet()
            model.fit(df_item)

            future   = model.make_future_dataframe(periods=120, freq='D')
            forecast = model.predict(future)

            # Formating the resulting datadrame and converting it to Tuple
            forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            forecast = forecast.set_index('ds').join(df_item.set_index('ds'), how='left')
            forecast['forecastDate'] = forecast.index.strftime("%Y-%m-%d")
            forecast['item'] = item
            forecast = forecast[['forecastDate', 'yhat', 'yhat_lower', 'yhat_upper', 'y', 'item']]
            forecast = forecast.replace({float('nan'): None})
            
            # Droping 1/3 of the forecasted values, the original data doesnt have sales on all days, this qty has been calcualted
            qty_to_drop = 30
            null_rows = forecast[forecast['y'].isnull()].index
            rows_to_drop = np.random.choice(null_rows, qty_to_drop, replace=False)
            forecast.loc[rows_to_drop, 'yhat'] = None
            
            forecastTuple = list(forecast.itertuples(index=False, name=None))

            insertSQL = f"""
                    INSERT INTO analyticsdata.{tableName} (forecastDate, forecastedValue, lowerLimit, upperLimit, realValue, itemID)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """
            cursor.executemany(insertSQL, forecastTuple)

            connection.commit()

        cursor.close()
        connection.close()
    

    @task()
    def dropTableValues():
       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute("DELETE FROM analyticsdata.forecast")

       connection.commit()
       cursor.close()
       connection.close()

    
    @task()
    def insertValues(tableName: str):

       pg_hook = PostgresHook(postgres_conn_id='postgreProd')
       connection = pg_hook.get_conn()
       cursor = connection.cursor()

       cursor.execute(f"""
            INSERT INTO analyticsdata.forecast
            SELECT * FROM analyticsdata.{tableName};
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
            DROP TABLE IF EXISTS analyticsdata.{tableName}; 
            """)

       connection.commit()
       cursor.close()
       connection.close()
        

    tableName = createTempTable()

    calculateForecast(tableName) >> dropTableValues() >> insertValues(tableName) >> dropTempTable(tableName)


forecast = forecast()