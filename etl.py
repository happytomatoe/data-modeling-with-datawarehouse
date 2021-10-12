import configparser
import json
import multiprocessing
import os
import time
from urllib.parse import urlparse

import boto3
import psycopg2
from botocore.exceptions import ClientError

from sql_queries import insert_table_queries, copy_table_queries

MANIFEST_FILE_NAME = "manifest.json"

my_bucket_name = "udacity-data-modelling"


def time_it(function):
    start_time = time.time()
    res = function()
    print("--- %s seconds ---" % (time.time() - start_time))
    return res


# idea from https://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
class Processor(object):
    """Process the data and save it to database."""

    def __init__(self, config, bucket_name, prefix):
        """Initialize the class with 'global' variables"""
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.config = config

    def __call__(self, additional_prefix):
        session = boto3.session.Session()

        s3 = session.resource('s3', region_name='us-east-1',
                              aws_access_key_id=self.config['AWS']['KEY'],
                              aws_secret_access_key=self.config['AWS']['SECRET'])
        bucket = s3.Bucket(self.bucket_name)
        # """Do something with the cursor and data"""
        return [f"s3://{o.bucket_name}/{o.key}" for o in
                bucket.objects.filter(Prefix=self.prefix + "/" + additional_prefix)]


def create_manifest_file(config):
    a = [chr(x) for x in range(ord('A'), ord('Z') + 1)]

    data_path = config['S3']['SONG_DATA'].strip("'")
    print(data_path)
    # from https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path
    o = urlparse(data_path, allow_fragments=False)
    print(o)
    bucket_name = o.netloc
    print("Bucket ", bucket_name)
    prefix = o.path.lstrip('/')
    print("Prefix ", prefix)

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    res = time_it(lambda: pool.map(Processor(config, bucket_name, prefix), a))

    pool.close()
    pool.join()
    print(f"Saving {MANIFEST_FILE_NAME}")
    manifest = {
        'entries': [{'url': path, "mandatory": True} for arr in res for path in arr]}
    with  open(MANIFEST_FILE_NAME, "w") as f:
        json.dump(manifest, f)


def load_staging_tables(cur, conn, config):
    # Advice from https://stackoverflow.com/questions/54528567/how-do-i-load-large-number-of-small-csv-files-from-s3-to-redshift

    # create_manifest_file(config)
    # upload_manifest(config)

    for query in copy_table_queries:
        print("Executing query")
        print(query)
        # TODO: finish him
        time_it(lambda: cur.execute(query))
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
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn, config)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
