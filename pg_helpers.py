import psycopg2
import os


def get_pg_uri():
    """
    Extracts PostgreSQL URI from the environment variable named POSTGRES_URI
    """
    if not os.environ.get('POSTGRES_URI'):
        raise RuntimeError('The POSTGRES_URI config variable is not set.')

    return os.environ.get('POSTGRES_URI')


def get_pg_connection():
    """ Establish connection to PostgreSQL database using the URI stored in the
    environment"""
    try:
        connection = psycopg2.connect(get_pg_uri())
        return connection

    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(f"Error while connecting to PostgreSQL : {error}")


def setup_pg(pg_conn):
    """ Creates a table for our PostgreSQL database if it doesn't exist"""
    try:
        cursor = pg_conn.cursor()

        # create table
        cursor.execute("""CREATE TABLE IF NOT EXISTS account(
                        first_name VARCHAR (255) NOT NULL,
                        last_name VARCHAR (255) NOT NULL,
                        email VARCHAR (355) NOT NULL,
                        height real NOT NULL);
                        """)
        cursor.close()
        pg_conn.commit()

    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(f"Error setting up table for PG database: {error}")
