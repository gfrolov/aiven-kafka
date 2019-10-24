import kafka_helpers
import helpers
import getopt
import os
import sys
import logging
import logging.config
from kafka.errors import KafkaError


def usage():
    print("Usage: python producer.py [-l -v]")


def run_producer(logger):

    try:
        producer = kafka_helpers.get_kafka_producer()
        logger.debug("Created Kafka Producer")

        for i in range(100):
            message = "message number {}".format(i)
            producer.send("times-topic", message)
        logger.info("Sent 100 messages")
        producer.flush()
    except KafkaError as e:
        logger.error("Got an error while sending messages")
        logger.error(e)
    except Exception as e:
        logger.error(f"Caught another error: {e}")


def main(argv):

    try:
        opts, args = getopt.getopt(argv, ":lv")
        file_name = None
        log_level = logging.ERROR
        for opt, arg in opts:
            if opt == "-l":
                file_name = "producer.log"
            elif opt == "-v":
                log_level = logging.DEBUG

        logger = helpers.create_logger(file_name, log_level)
        run_producer(logger)

    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    logging.debug("Exiting")


if __name__ == "__main__":

    main(sys.argv[1:])
