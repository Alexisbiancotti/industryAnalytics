import os

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

def readKafka():

    env = StreamExecutionEnvironment.get_execution_environment()

    # absolute path to jar files using relative path and os
    kafkaJar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                'flink-sql-connector-kafka-1.16.3.jar')
    
    jsonJar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                'flink-json-1.16.3.jar')
    
    jdbcJar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                'flink-connector-jdbc-1.16.3.jar')
    
    # adding the files to the enviroment
    env.add_jars("file://{}".format(kafkaJar), "file://{}".format(jsonJar), "file://{}".format(jdbcJar))

    env.add_classpaths("file://{}".format(kafkaJar), "file://{}".format(jsonJar), "file://{}".format(jdbcJar))

    # creating the json schema of the topic in order to be read
    row_type_info = Types.ROW_NAMED(['createdAt', 'mach', 'temp'], [Types.BIG_DEC(), Types.STRING(), Types.FLOAT()])

    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

    # creating the source from kafka topic
    kafka_consumer = FlinkKafkaConsumer(
        topics='testIoTSensor',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'broker:29092', 'group.id': 'pyflink'})

    ds = env.add_source(kafka_consumer)

    # printing the results
    ds.print()

    jdbcConn = JdbcSink.sink(
        "insert into test (createdAt, mach, temp) values (?, ?, ?)",
        row_type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:postgresql://postgres:5432/airflow?currentSchema=prod')
            .with_driver_name('org.postgresql.Driver')
            .with_user_name('alexis')
            .with_password('alexis6895')
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(200)
            .with_max_retries(5)
            .build()
    )

    ds.add_sink(jdbcConn)

    # submit job
    env.execute()
    

if __name__ == "__main__":
     
     readKafka()