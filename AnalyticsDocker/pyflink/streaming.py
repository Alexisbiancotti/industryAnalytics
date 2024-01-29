import os

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

def readKafka():

    env = StreamExecutionEnvironment.get_execution_environment()

    # absolute path to jar files using relative path and os
    kafkaJar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                'flink-sql-connector-kafka-1.17.2.jar')
    
    jsonJar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                'flink-json-1.18.1.jar')
    
    # adding the files to the enviroment
    env.add_jars("file://{}".format(kafkaJar), "file://{}".format(jsonJar))

    env.add_classpaths("file://{}".format(kafkaJar), "file://{}".format(jsonJar))

    # creating the json schema of the topic in order to be read
    row_type_info = Types.ROW_NAMED(['createdAt', 'mach', 'temp'], [Types.BIG_INT(), Types.STRING(), Types.FLOAT()])

    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

    # creating the source from kafka topic
    kafka_consumer = FlinkKafkaConsumer(
        topics='testIoTSensor',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'broker:29092', 'group.id': 'pyflink'})

    ds = env.add_source(kafka_consumer)

    # printing the results





if __name__ == "__main__":
     
     readKafka()