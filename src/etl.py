import configparser
import os
import time

import boto3
import psycopg2

from sql_queries import insert_table_queries, copy_table_queries, staging_events_copy, TableNames


def time_it(function):
    start_time = time.time()
    res = function()
    print("--- %s seconds ---" % (time.time() - start_time))
    return res


def load_staging_tables(cur, conn, config):
    for query in copy_table_queries:
        print("Executing query")
        print(query)
        time_it(lambda: cur.execute(query))
        conn.commit()


def load_staging_tables_in_parts(cur, conn, config):
    a = [chr(x) for x in range(ord('A'), ord('Z') + 1)]

    print("Executing query")
    print(staging_events_copy)
    time_it(lambda: cur.execute(staging_events_copy))
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
        time_it(lambda: cur.execute(q))
        conn.commit()


def upload_manifest(config):
    print(f"Uploading {MANIFEST_FILE_NAME} to s3://{my_bucket_name}")
    s3 = boto3.resource('s3', region_name='us-west-2',
                        aws_access_key_id=config['AWS']['KEY'],
                        aws_secret_access_key=config['AWS']['SECRET'])
    try:
        s3.meta.client.head_bucket(Bucket=my_bucket_name)
    except ClientError:
        print("Creating bucket ", my_bucket_name)
        location = {'LocationConstraint': 'us-west-2'}
        s3.create_bucket(Bucket=my_bucket_name, CreateBucketConfiguration=location)
    s3.meta.client.upload_file(os.getcwd() + "/" + MANIFEST_FILE_NAME, my_bucket_name,
                               'manifest.json')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Executing query {query}")
        time_it(lambda: cur.execute(query))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_staging_tables(cur, conn, config)
    # load_staging_tables_in_parts(cur, conn, config)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
