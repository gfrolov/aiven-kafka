import kafka_helpers
import helpers
import getopt
import os
import sys
import logging
import logging.config
from kafka.errors import KafkaError
import json


def usage():
    print("Usage: python producer.py [-l -v]")


def get_data():

    try:
        data_file = open("accounts.json", "r")
        data = json.loads(data_file.read())
        return data
    except IOError as e:
        raise RuntimeError(f"Got an error trying to read data: {e}")


def run_producer(topic_name, logger):
    """ This function will create a Kafka producer, then read JSON data
    from file and send messages to the specific topic """

    try:
        # create kafka producer
        producer = kafka_helpers.get_kafka_producer()
        logger.debug("Created Kafka Producer")

        # read data from file and send each item there as a separate msg
        data = get_data()
        for item in data:
            logger.debug(item)
            producer.send(topic_name, item)
        logger.info(f"Sent {len(data)} messages")
        producer.flush()
    except KafkaError as e:
        logger.error("Got an error while sending messages")
        logger.error(e)
    except Exception as e:
        logger.error(f"Encountered an error: {e}")


def main(argv):
    # get command line arguments
    try:
        opts, args = getopt.getopt(argv, "t:lv")
        file_name = None
        log_level = logging.ERROR
        topic_name = None
        for opt, arg in opts:
            if opt == "-t":
                topic_name = arg
            elif opt == "-l":
                file_name = "producer.log"
            elif opt == "-v":
                log_level = logging.DEBUG

        # make sure required argument is speficied
        if(topic_name is None):
            logger.error("Missing required arguments")
            usage()
            sys.exit()

        logger = helpers.create_logger(file_name, log_level)
        run_producer(topic_name, logger)

    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    logging.debug("Exiting")


if __name__ == "__main__":

    main(sys.argv[1:])
