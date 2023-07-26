from fastapi import FastAPI
import numpy as np
import bentoml
from bentoml.io import NumpyNdarray

from pyflink.common import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer

clf_runner = bentoml.sklearn.get("test_clf:latest").to_runner()
svc = bentoml.Service("iris_classifier", runners=[clf_runner])

bento_api = FastAPI()
svc.mount_asgi_app(bento_api)

@bento_api.on_event('startup')
def on_startup():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get Data from Kafka
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()
    env.add_source(kafka_consumer).print()

    # Make Derived Features
    # Preprocessing
    # Prediction
    # XAI
    # Input Results to Kafka
    pass
