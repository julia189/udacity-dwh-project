import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes each query in copy_table_queries and commits each transaction.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes each query in insert_table_queries and commits each transaction.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():


    # parsing the data warehouse configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # setting up the connection 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # loading data into staging tables
    load_staging_tables(cur, conn)
    # inserting data into star schema tables
    insert_tables(cur, conn)

    # closing dwh connection
    conn.close()


if __name__ == "__main__":
    main()