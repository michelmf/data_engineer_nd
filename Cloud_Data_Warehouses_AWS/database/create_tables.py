import configparser
import psycopg2
from   .sql_queries import drop_table_queries, create_table_queries

def drop_tables(cur, conn):
    print('Dropping tables')
    for query in drop_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    print('Creating new tables')
    for query in create_table_queries:
        print(f'Executing {query}')
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('../dwh.cfg')
    
    print('Connecting to redshift cluster ...')
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        # If the connection is ok
        print('Connected to Redshift.')

    except Exception as e:
        print(f'Error while connecting: {e}')  

    try:
        # Dropping and Creating for testing purposes
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        conn.close()
    
    except Exception as e:
        print(f'Error while executing the queries: {e}')
    

if __name__ == "__main__":
    main()