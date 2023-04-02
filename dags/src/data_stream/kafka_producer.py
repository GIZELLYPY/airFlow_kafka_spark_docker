from kafka import KafkaProducer
from time import sleep
from json import dumps
import os
import logging
from airflow.models import Variable

kafka = Variable.get("BOOTSTRAP_SERVERS")


def generate_stream(**kwargs):
    """Write events to a Kafka cluster. A producer partitioner maps
    each message from the train.csv file to a topic partition named by
    Transactions, and the producer sends a produce request to the leader
    of that partition. The partitioners shipped with Kafka guarantee
    that all messages with the same non-empty key will be sent to the
    same partition.

       Parameters
        ----------
        path_stream : :str
            Path were the train.csv file is stored
        Topic : :str
            Transactions. This is about taxi ride transactions on NYC.

    """

    input_file_loc = kwargs["path_stream"]
    topic = kwargs["Topic"]
    producer = KafkaProducer(
        bootstrap_servers=kafka,
        value_serializer=lambda x: dumps(x).encode("utf-8"),
        linger_ms=10,
        api_version=(1, 4, 6),
    )

    with open(os.getcwd() + input_file_loc, mode="r") as f:
        logging.info("Reading....", os.getcwd() + input_file_loc)
        for line in f:
            json_comb = dumps(line)
            producer.send(topic, value=json_comb)
            sleep(1)
    producer.flush()
    producer.close()

    logging.info("Finished")
