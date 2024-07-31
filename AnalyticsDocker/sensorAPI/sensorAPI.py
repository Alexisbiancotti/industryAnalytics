from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import psycopg2
import os

app = FastAPI()

# Database connection settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_PORT = os.getenv("DB_PORT", "5432")

# Pydantic model for input validation
class SensorData(BaseModel):
    createdAt: int
    mach: str
    temp: float

# Function to insert data into PostgreSQL
def insert_data(sensor_data: SensorData):

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO analyticsdata.sensorDataAPI (createdAt, mach, temp)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (sensor_data.createdAt, sensor_data.mach, sensor_data.temp))
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/data")
def receive_data(sensor_data: SensorData):
    insert_data(sensor_data)
    return {"message": "Data received successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)