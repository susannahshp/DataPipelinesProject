import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This function iterates through the drop_table_queires and executes each query and drops the tables.'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''This function iterates through the create_table_queries and executes each query and creates the tables.'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''This is the main function and it creates an object of a configparser and reads though dwh.cfg file. Then it connects to a AWS Redshift cluster by creating a psycopg2 object using the parameters of the configuration file. It runs the drop_tables and create_table function and closes the connection.'''
    config = configparser.ConfigParser()
    config.read('aws.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()