import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
     """drops pre existing tables
    Args:
        cur (cursor): cursor to execute queries
        conn: open connection 
    """
    try:
        print("dropping any pre existing tables")
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_tables(cur, conn):
     """creates all required tables specified in sql_queries
    Args:
        cur (cursor): cursor to execute queries
        conn: open connection 
    """
    try:
        print("creating all tables")
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_dbObjects(cur, conn):
    
    drop_tables(cur, conn)
    create_tables(cur, conn)


if __name__ == "__main__":
    create_dbObjects(cur, conn)