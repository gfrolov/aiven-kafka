import kafka_helpers
import helpers
import pg_helpers
import getopt
import os
import sys
import logging
import logging.config


def usage():
    """helper function that prints the correct usage of the consumer app"""
    print(
        "Usage: python consumer.py -t topic_name -g group_id -c client_id [-l -v]")


def run_consumer(logger, topic_name):
    """Runs the main consumer app """

    # establish Posgres connection and setup database if not already
    pg_conn = pg_helpers.get_pg_connection()
    pg_helpers.setup_pg(pg_conn)

    # get kafka consumer to receive messages
    consumer = kafka_helpers.get_kafka_consumer(topic_name)
    logger.debug("Created Kafka consumer")
    data = []

    # poll for the messages and store them in a list
    for _ in range(2):
        raw_msgs = consumer.poll(timeout_ms=5000)
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                logger.debug("Received: {}".format(msg.value))
                data.append(msg.value)
    logger.info(f"Received {len(data)} messages")
    # if any messages received, then insert into database
    if(len(data) > 0):
        try:
            cursor = pg_conn.cursor()
            for item in data:
                # create query based on the data received
                query = f"""INSERT INTO account (first_name, last_name, email, height)
                        VALUES ('{item['first_name']}', '{item['last_name']}', '{item['email']}', {item['height']});"""
                cursor.execute(query)
            cursor.close()
            pg_conn.commit()
        except (Exception, psycopg2.Error) as error:
            raise RuntimeError(
                f"Error while inserting into PG database: {error}")
        finally:
            pg_conn.close()

    consumer.commit()


def main(argv):

    # get command line arguments
    try:
        opts, args = getopt.getopt(argv, "t:g:c:lv")
        file_name = None
        log_level = logging.ERROR
        topic_name = None
        group_id = None
        client_id = None
        for opt, arg in opts:
            if opt == "-t":
                topic_name = arg
            if opt == "-l":
                file_name = "consumer.log"
            elif opt == "-v":
                log_level = logging.DEBUG
            elif opt == "-g":
                group_id = arg
            elif opt == "-c":
                client_id = arg
        logger = helpers.create_logger(file_name, log_level)
        # make sure required arguments have been set
        if(topic_name is None or group_id is None or client_id is None):
            logger.error("Missing required arguments")
            usage()
            sys.exit()

    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    run_consumer(logger, topic_name)

    logger.debug("Exiting")


if __name__ == "__main__":

    main(sys.argv[1:])
