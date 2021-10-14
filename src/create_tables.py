import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, drop_non_staging_tables_queries, \
    create__non_staging_tables_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print("executing query ", query)
        cur.execute(query)
        conn.commit()

def drop_non_staging_tables(cur, conn):
    for query in drop_non_staging_tables_queries:
        print("executing query ", query)
        cur.execute(query)
        conn.commit()


def create_non_staging_tables(cur, conn):
    for query in create__non_staging_tables_queries:
        print("executing query ", query)
        cur.execute(query)
        conn.commit()



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop_tables(cur, conn)
    # create_tables(cur, conn)

    drop_non_staging_tables(cur, conn)
    create_non_staging_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()