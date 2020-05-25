import psycopg2
import configparser

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        if cur: print('Connected correctly!')

        conn.close()

    except Exception as e:
        print(e)