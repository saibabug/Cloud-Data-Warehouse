import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, validation_queries
from create_resources import create_resources
from create_tables import create_dbObjects
from validation import analyse_data
from shutdown_resources import shutdown_resources


def load_staging_tables(cur, conn):
    """Loads data into staging tables
    Args:
        cur (cursor): cursor to execute queries
        conn: open connection 
    """
    try:
        print("Loading staging tables started\n")
        for query in copy_table_queries:
            print("Executing query: "+query)
            cur.execute(query)
            conn.commit()
        print("Loading staging tables completed\n")
    except Exception as e:
        print(e)


def insert_tables(cur, conn):
    """Loads data into dimension tables
    Args:
        cur (cursor): cursor to execute queries
        conn: open connection 
    """
    
    try:
        print("inserting data into dimension tables started\n")
        for query in insert_table_queries:
            print("Executing query: "+query)
            cur.execute(query)
            conn.commit()
        print("inserting data into dimension tables completed\n")
    except Exception as e:
        print(e)


def main(): 
    create_resources()
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host = config.get('REDSHIFT', 'HOST')
    db_name = config.get('REDSHIFT', 'DB_NAME')
    db_username = config.get('REDSHIFT', 'DB_MASTER_USER')
    db_password = config.get('REDSHIFT', 'DB_MASTER_PASSWORD')
    port = config.getint('REDSHIFT', 'DB_PORT')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, db_username, db_password, port))
    cur = conn.cursor()  
    create_dbObjects(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    analyse_data(cur, conn)
    conn.close()
    shutdown_resources()


if __name__ == "__main__":
    main()