import configparser
import time

import psycopg2

from sql_queries import insert_table_queries, copy_table_queries, staging_events_copy, TableNames

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Executing query")
        print(query)
        cur.execute(query)
        conn.commit()


def load_staging_tables_in_parts(cur, conn, config):
    a = [chr(x) for x in range(ord('A'), ord('Z') + 1)]

    print("Executing query")
    print(staging_events_copy)
    cur.execute(staging_events_copy)
    conn.commit()
    for x in a:
        print("Executing query")

        q = (f"""
        copy {TableNames.staging_songs} 
        from {{}}
        iam_role {{}}
        COMPUPDATE OFF STATUPDATE OFF
        format as json 'auto'
        truncatecolumns
        BLANKSASNULL
        ;
        """).format(config['S3']['SONG_DATA'][:-1] + "/" + x + "'", config['IAM_ROLE']['ARN'])
        print(q)
        cur.execute(q)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Executing query {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    # load_staging_tables_in_parts(cur, conn, config)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
