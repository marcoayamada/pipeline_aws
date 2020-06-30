import psycopg2
import os


def execute_single_sql(dbname, host, port, user, password, sql):
    try:
        print('Opening connection to dataabase...')
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password)
        cur = conn.cursor()

        cur.execute(sql)

        conn.commit()
        conn.close()
        print("Done!")
    except Exception as e:
        print(e)


def execute_batch_sql(dbname, host, port, user, password, directory):
    try:
        print('Opening connection to database...')
        print('  Opening directory: {}'.format(directory))
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password)
        cur = conn.cursor()

        for f in sorted(os.listdir(directory)):
            print('    Executing file: {}'.format(f))
            script = open("{}/{}".format(directory, f), "r").read()
            cur.execute(script)

        conn.commit()
        conn.close()
        print("Done!")
    except Exception as e:
        print(e)
