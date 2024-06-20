import boto3 
import time
import pandas as pd
from io import StringIO
import redshift_connector

AWS_ACCES_KEY = "AKIA47CRU6EFD575BE6N"
AWS_SECRET_KEY = "SECRET_KEY"
AWS_REGION = "eu-north-1"
SCHEMA_NAME = "covid_19"
S3_STAGING_DIR = "s3://test-bucket-covid/output/"
S3_BUCKET_NAME = "test-bucket-covid"
S3_OUTPUT_DIRECTORY = "output"

athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCES_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

Dict = {}

def download_and_load_query_result(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            #lis uniquement les 1000 premiere lignees
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCES_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location
    )
    return pd.read_csv(temp_file_location)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhu",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
enigma_jhu = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM json", #json = usa-hospital-bed
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
usa_hospital_bed = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_states",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
nytimes_data_in_usa_us_states = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_conty",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
nytimes_data_in_usa_us_county = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_states_dailystates_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_covid_19_testing_data_states_daily_states_daily = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_dataus_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_covid_19_testing_data_us_daily = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_dataus_total_latest",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
rearc_covid_19_testing_data_us_total_latest = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datacountrycode",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_data_countrycode = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datacountypopulation",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_data_countypopulation = download_and_load_query_result(athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datastate_abv",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
static_data_state_abv = download_and_load_query_result(athena_client, response)

#correction header abv
new_header = static_data_state_abv.iloc[0] #prend la premiere ligne
static_data_state_abv = static_data_state_abv[1:] #skip la premiere ligne inutile
static_data_state_abv.columns = new_header #met la premiere ligne copié en header

#table infoCovid
infoCovid_1 = enigma_jhu[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
infoCovid_2 = rearc_covid_19_testing_data_states_daily_states_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
infoCovid = pd.merge(infoCovid_1, infoCovid_2, on='fips', how='inner')

#table region
region_1 = enigma_jhu[['fips','province_state','country_region','latitude','longitude']]
region_2 = nytimes_data_in_usa_us_county[['fips', 'county', 'state']]
region = pd.merge(region_1, region_2, on='fips', how='inner')

hospital = usa_hospital_bed[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]

date = rearc_covid_19_testing_data_states_daily_states_daily[['fips','date']].copy()

date['date'] = pd.to_datetime(date['date'], format='%Y%m%d') #converti le format de la date

date['year'] = date['date'].dt.year
date['month'] = date['date'].dt.month
date["day_of_week"] = date['date'].dt.dayofweek

print(region.head)


bucket = 'projet-covid'
csv_buffer = StringIO()

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=AWS_ACCES_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

infoCovid.to_csv(csv_buffer)
s3_resource.Object(bucket, 'output/infoCovid.csv').put(Body=csv_buffer.getvalue())


# csv_buffer.truncate(0)
# csv_buffer.seek(0)
# region.to_csv("region.csv", index=False)
# s3_resource.Object(bucket, 'output/region.csv').put(Body=csv_buffer.getvalue())

csv_buffer.truncate(0)
csv_buffer.seek(0)
date.to_csv(csv_buffer)
s3_resource.Object(bucket, 'output/date.csv').put(Body=csv_buffer.getvalue())

csv_buffer.truncate(0) 
csv_buffer.seek(0)
hospital.to_csv(csv_buffer)
s3_resource.Object(bucket, 'output/hospital.csv').put(Body=csv_buffer.getvalue())


dateSql = pd.io.sql.get_schema(date.reset_index(), 'date') # equivalent à CREATE TABLE x
covidSql = pd.io.sql.get_schema(infoCovid.reset_index(), 'infoCovid')
regionSql = pd.io.sql.get_schema(region.reset_index(), 'region')
hospitalSql = pd.io.sql.get_schema(hospital.reset_index(), 'hospital')
print(''.join(hospitalSql))