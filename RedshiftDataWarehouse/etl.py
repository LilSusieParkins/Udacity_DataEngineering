import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copy data log and song json files into staging tables. 
    '''
    stagingIdx = 1
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('{} of {} copy tasks complete.'.format(stagingIdx, len(copy_table_queries)))
        stagingIdx += 1


def insert_tables(cur, conn):
    '''
    Insert data from staging tables into star schema data model. 
    '''
    insertIdx = 1
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('{} of {} insert tasks complete.'.format(insertIdx, len(insert_table_queries)))
        insertIdx += 1


def main():
    '''
    Connect to Redshift cluster and run functions to stage data and populate star schema data model. 
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST=config.get("DWH","DWH_HOST")
    DB_NAME=config.get("DWH","DWH_DB")
    DB_USER=config.get("DWH","DWH_DB_USER")
    DB_PASSWORD=config.get("DWH","DWH_DB_PASSWORD")
    DB_PORT=config.get("DWH","DWH_PORT")
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=HOST, port =DB_PORT)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    '''
    Guard to prevent code from executing if imported by another script. 
    '''
    main()