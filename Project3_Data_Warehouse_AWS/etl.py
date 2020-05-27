import configparser
import psycopg2
from time import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function that load the data directly from JSON formats into the S3 buckets.
    """
    for query in copy_table_queries:
        print('Executing: {}'.format(query))
        t1 = time()
        cur.execute(query)
        conn.commit()
        print("    Tot time: {time:.2f} seconds".format(time=time()-t1))
        print("="*40)


def insert_tables(cur, conn):
    """
    Function that insert data from the two staging tables
    into the five final tables.
    """
    for query in insert_table_queries:
        print('Executing: {}'.format(query))
        t1 = time()
        cur.execute(query)
        conn.commit()
        print("    Tot time: {time:.2f} seconds".format(time=time()-t1))
        print("="*40)


def main():
    t0 = time()
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print("Ended in: {time:.2f} seconds".format(time=time()-t0))


if __name__ == "__main__":
    main()