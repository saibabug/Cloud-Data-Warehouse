from sql_queries import validation_queries

def execute_query(cur, conn, query):
    """Execute SQL query on Redshift
    Args:
        cur (cursor object): Cursor Object used to retrieve rows from SQL Engine
        conn (connection object): Connection to SQL Engine
        query (string): SQL Query
    """
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except Exception as e:
        print(e)  

def analyse_data(cur,conn):
    """Execute validation SQL query on Redshift and prints results on screen
    Args:
        cur (cursor object): Cursor Object used to retrieve rows from SQL Engine
        conn (connection object): Connection to SQL Engine
        query (string): SQL Query
    """
    try:
        print("Executing validation of data loaded\n")
        for query in validation_queries:
            print("validation query:\n")
            print(query)
            print("\n") 
            execute_query(cur,conn,query)        
        print("Executing validation of data load completed\n")
    except Exception as e:
        print(e)