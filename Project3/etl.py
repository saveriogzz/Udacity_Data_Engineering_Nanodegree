import configparser
import psycopg2
from time import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Executing: {}'.format(query))
        t1 = time()
        cur.execute(query)
        conn.commit()
        print("    Tot time: {time:.2f}".format(time=time()-t1))
        print("="*40)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Executing: {}'.format(query))
        t1 = time()
        cur.execute(query)
        conn.commit()
        print("    Tot time: {time:.2f}".format(time=time()-t1))
        print("="*40)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()