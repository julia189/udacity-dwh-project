import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes each query in the drop_table_queries list and commits each transaction.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes each query in the create_table_queries list and commits each transactions.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    # parsing the data warehouse configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # setting up the connection 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # dropping tables from schema
    drop_tables(cur, conn)
    # creating tables in schema 
    create_tables(cur, conn)

    # closing the dwh connection
    conn.close()


if __name__ == "__main__":
    main()