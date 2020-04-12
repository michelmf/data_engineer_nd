# Main file of the project.
import configparser
import psycopg2

# Queries to be executed
from database.sql_queries   import drop_table_queries, create_table_queries, insert_table_queries, copy_table_queries
from database.create_tables import drop_tables, create_tables

# ETL step
from etl.etl import load_staging_tables, insert_tables

def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Inserting into staging strings the values needed


    print('Starting Project . . .\n\n\n')

    # Second Step: Creation of Schema and Tables (IF no schema is specified, Schema is public)
    
    print('Connecting to redshift cluster ...')
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        # If the connection is ok
        print('Connected to Redshift.')

        # Dropping and Creating tables
        drop_tables(cur, conn)
        create_tables(cur, conn)

        # ETL Process on staging data
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        
        conn.close()
    
    except Exception as e:
        print(f'Error while connecting to cluster: {e}')
        exit()

    print('Done...')
    
if __name__ == '__main__':
    
    main()