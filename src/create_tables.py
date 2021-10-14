import configparser

import psycopg2

from sql_queries import CREATE_TABLE_QUERIES, DROP_TABLE_QUERIES


def drop_tables(cur, conn):
    """
    Drops existing tables
    :param cur: db cursor
    :param conn: db connection
    """
    for query in DROP_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables
    :param cur: db cursor
    :param conn: db connection
    """
    for query in CREATE_TABLE_QUERIES:
        print("executing query ", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
