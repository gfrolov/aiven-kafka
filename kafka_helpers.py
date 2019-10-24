from kafka import KafkaProducer, KafkaConsumer
import os

"""
Adapted from Heroku Kafka Helpers
"""


def get_kafka_broker():
    """
    Parses the KAKFA_URL and returns a hostname: port that kafka - python expects.
    Expects to be stored in environ var as "brokername,port"
    """
    if not os.environ.get('KAFKA_URL'):
        raise RuntimeError('The KAFKA_URL config variable is not set.')

    kafka_url = os.environ.get('KAFKA_URL').split(',')
    return "{}:{}".format(kafka_url[0], kafka_url[1])


def get_kafka_ssl_info():
    """
    Expects the following variables to be set KAFKA_CA, KAFKA_SERVICE_CERT, KAFKA_SERVICE_KEY
    to file locations of each of the corresponding files
    KAFKA_CA - ca.pem
    KAFKA_SERVICE_CERT - service.cert
    KAFKA_SERVICE_KEY - service.key
    """

    if not os.environ.get('KAFKA_CA'):
        raise RuntimeError('The KAFKA_CA config variable is not set.')
    if not os.environ.get('KAFKA_SERVICE_CERT'):
        raise RuntimeError(
            'The KAFKA_SERVICE_CERT config variable is not set.')
    if not os.environ.get('KAFKA_SERVICE_KEY'):
        raise RuntimeError('The KAFKA_SERVICE_KEY config variable is not set.')

    ssl_info = {}
    ssl_info["ssl_cafile"] = os.environ["KAFKA_CA"]
    ssl_info["ssl_certfile"] = os.environ["KAFKA_SERVICE_CERT"]
    ssl_info["ssl_keyfile"] = os.environ["KAFKA_SERVICE_KEY"]

    return ssl_info


def get_kafka_producer():
    """
    Return a KafkaProducer that uses the configured Kafka broker and corresponding SSL info
    """
    ssl_info = get_kafka_ssl_info()

    producer = KafkaProducer(
        bootstrap_servers=get_kafka_broker(),
        security_protocol="SSL",
        ssl_cafile=ssl_info["ssl_cafile"],
        ssl_certfile=ssl_info["ssl_certfile"],
        ssl_keyfile=ssl_info["ssl_keyfile"],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    return producer


def get_kafka_consumer(topic=None):
    """
    Return a KafkaConsumer that uses the configured Kafka broker and corresponding SSL info
    """

    ssl_info = get_kafka_ssl_info()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=get_kafka_broker(),
        security_protocol='SSL',
        ssl_cafile=ssl_info["ssl_cafile"],
        ssl_certfile=ssl_info["ssl_certfile"],
        ssl_keyfile=ssl_info["ssl_keyfile"],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    return consumer
