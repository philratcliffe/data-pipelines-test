import configparser
import csv

import boto3
import pymysql


def get_db_connection():
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    hostname = parser.get("mysql_config", "hostname")
    port = parser.get("mysql_config", "port")
    username = parser.get("mysql_config", "username")
    dbname = parser.get("mysql_config", "database")
    password = parser.get("mysql_config", "password")

    conn = pymysql.connect(
        host=hostname, user=username, password=password, db=dbname, port=int(port)
    )

    if conn is None:
        print("Error connecting to the MySQL database")
    else:
        print("MySQL connection established!")

    return conn


def upload_to_s3(filename):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    s3_file = filename
    s3.upload_file(filename, bucket_name, s3_file)

def extract_mysql_full():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Orders")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


rows = extract_mysql_full()
local_filename = "order_extract.csv"
with open(local_filename, "w") as fp:
    csv_w = csv.writer(fp, delimiter="|")
    csv_w.writerows(rows)


upload_to_s3(local_filename)
