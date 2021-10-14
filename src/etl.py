import configparser

import psycopg2

from sql_queries import INSERT_TABLE_QUERIES, COPY_TABLE_QUERIES


def load_staging_tables(cur, conn):
    """
    Load data into staging tables
    :param cur: db cursor
    :param conn: db connection
    """
    for query in COPY_TABLE_QUERIES:
        print("Executing query")
        print(query)
        cur.execute(query)
        conn.commit()


def merge_tables(cur, conn):
    """
    Doing upsert, merges staging and target tables
    :param cur: db cursor
    :param conn: db connection
    """
    for query in INSERT_TABLE_QUERIES:
        print(f"Executing query {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    merge_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
