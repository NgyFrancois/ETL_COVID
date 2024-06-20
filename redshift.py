import boto3 
import time
import pandas as pd
from io import StringIO
import redshift_connector

conn = redshift_connector.connect(
    host='redshift-cluster-2.ck9isbajzex0.eu-north-1.redshift.amazonaws.com',
    database='dev',
    port=5439,
    user='awsuser',
    password='mot_de_passe' 
)

conn.autocommit = True

cursor=redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "date" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute("""
CREATE TABLE "infoCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" REAL,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

cursor.execute("""
CREATE TABLE "region" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "hospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

cursor.execute("""
copy date from 's3://projet-covid/output/date.csv'
credentials 'aws_iam_role=arn:aws:iam::891376955658:role/s3-redshift-glue'
delimiter ','
region 'eu-north-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy infoCovid from 's3://projet-covid/output/infoCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::891376955658:role/s3-redshift-glue'
delimiter ','
region 'eu-north-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy hospital from 's3://projet-covid/output/hospital.csv'
credentials 'aws_iam_role=arn:aws:iam::891376955658:role/s3-redshift-glue'
delimiter ','
region 'eu-north-1'
IGNOREHEADER 1
""")

# cursor.execute("""
# copy region from 's3://projet-covid/output/region.csv'
# credentials 'aws_iam_role=arn:aws:iam::891376955658:role/s3-redshift-glue'
# delimiter ','
# region 'eu-north-1'
# IGNOREHEADER 1
# """)


