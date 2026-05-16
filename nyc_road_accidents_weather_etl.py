import logging
from datetime import datetime, timedelta
import io
import requests
import pandas as pd
from sodapy import Socrata
from calendar import monthrange
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from email_notifier import EmailNotificationLibrary
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator

# Configs and constants
OWNER = Variable.get("owner")
EMAIL = Variable.get("receiver_email")
SODA_DOMAIN = "data.cityofnewyork.us"
SODA_DATASET_ID = "bm4k-52h4"
SODA_APP_TOKEN = Variable.get("soda_app_token")
NYC_LAT, NYC_LON = 40.7128, -74.0060
GCS_BUCKET = "nyc-road-accidents-weather-correlation"
LAYER_1 = "raw"
SOURCE_1 = "accidents"
SOURCE_2 = "weather"
POSTGRES_CONN_ID = "postgres_supabase_etl"
SCHEMA = "dwh"
TARGET_TABLE = "road_accidents_weather_correlation_in_nyc_v2"

default_args = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    'email': EMAIL,
    'email_on_failure': True
}



def save_to_gcs(df, year, month, layer, source):
    # Serialize the dataframe to Parquet in-memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    data = buffer.getvalue() # pass the binary data directly to the data parameter

    # Define GCS Object Name (formerly S3 Key)
    GCS_OBJECT_NAME = f"{layer}/{source}/monthly_summary_{source}_{year}_{month:02d}.parquet"
    gcs_hook = GCSHook() # using google_cloud_default connection

    logging.info(f"Uploading to gs://{GCS_BUCKET}/{GCS_OBJECT_NAME}")

    # GCS upload method using the 'data' parameter for in-memory bytes
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=GCS_OBJECT_NAME,
        data=data,
        mime_type='application/octet-stream'
    )
    logging.info(f"✅ Monthly {source} summary for {month} saved to GCS successfully.")

    

def get_from_gcs_to_df(object_name):
    gcs_hook = GCSHook()
    logging.info(f"Downloading gs://{GCS_BUCKET}/{object_name}")
    
    try:
        # Download the file as bytes directly from GCS
        file_bytes = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=object_name)
        parquet_file_buffer = io.BytesIO(file_bytes)
    except Exception as e:
        raise Exception(f"Failed to download/access GCS file: {e}")

    logging.info("Reading Parquet into Pandas DataFrame...")
    # When Pandas reads Parquet via PyArrow, it only has to read the exact columns it needs (e.g., date and crashes_per_day). 
    # It physically skips over all other data, drastically reducing disk I/O and network transfer time from GCS. 
    # This makes data retrieval extremely fast.
    df = pd.read_parquet(parquet_file_buffer)
    logging.info(f"DataFrame ready. Rows: {len(df)}")
    return df


def fetch_last_month_accidents(**context):
    # 'data_interval_start' is the start of the period Airflow is currently processing
    # If the DAG is running in December, this will point to November
    # this allows to always fetch the "previous month" regardless of when the DAG runs. This ensures that if you re-run a task from 3 months ago,
    # it fetches the correct data for that time.
    logical_date = context['data_interval_start']
    year = logical_date.year
    month = logical_date.month

    logging.info(f"Generating road accidents summary for: {year}-{month}")
    
    client = Socrata(SODA_DOMAIN, SODA_APP_TOKEN)
    where_clause = f"date_extract_y(crash_date) = {year} AND date_extract_m(crash_date) = {month}"
    
    # we're grouping by each day of the month and counting the number of accidents per day
    # by performing the aggregation on the Socrata side, we reduce the data transferred over the network
    results = client.get(SODA_DATASET_ID, limit=50000, select="crash_date, count(crash_date) AS crashes_per_day", where=where_clause, group="crash_date")
    if not results:
        logging.warning("No records found for the target month.")
        raise AirflowException("Task failed. Aborting ...")

    # Process Data with Pandas
    df = pd.DataFrame.from_records(results)
    logging.info(f"Aggregated {len(df)} days of data.")

    save_to_gcs(df, year, month, LAYER_1, SOURCE_1)
    client.close()



    
def fetch_last_month_weather(**context):
    logical_date = context['data_interval_start']
    year = logical_date.year
    month = logical_date.month
    how_many_days = monthrange(year, month)[1] # we need to know how many days in this particular month
    start_date = f'{year}-{month:02d}-01'
    end_date = f'{year}-{month:02d}-{how_many_days}'
    logging.info(f"Getting weather data for {start_date}-{end_date}...")

    # Get weather data from Open-Meteo API

    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={NYC_LAT}&longitude={NYC_LON}&start_date={start_date}&end_date={end_date}&daily=weather_code,precipitation_sum,rain_sum,snowfall_sum,temperature_2m_mean,wind_speed_10m_mean,cloud_cover_mean,relative_humidity_2m_mean&timezone=America%2FNew_York"
    response = requests.get(url, timeout=30)
    if response.status_code == 200:
        data = response.json()
        dict = {
            "date": data['daily']['time'], 
            "wmo_code": data['daily']['weather_code'],
            "temperature_celcius": data['daily']['temperature_2m_mean'],
            "precipitation_mm": data['daily']['precipitation_sum'],
            "rain_mm": data['daily']['rain_sum'],
            "snowfall_cm": data['daily']['snowfall_sum'],
            "wind_speed_km/h": data['daily']['wind_speed_10m_mean'],
            "cloud_cover_%": data['daily']['cloud_cover_mean'],
            "humidity_%": data['daily']['relative_humidity_2m_mean']
        }
        df =pd.DataFrame(dict)
        logging.info(f"Aggregated {len(df)} days of weather data.")
        save_to_gcs(df,year,month,LAYER_1,SOURCE_2)
    else:
        logging.info(f"Failed to fetch weather data: Status code {response.status_code}")
        raise AirflowException("Task failed. Aborting ...")


def create_target_table_if_not_exists():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
        date DATE PRIMARY KEY,
        crashes_per_day INT,
        wmo_code INT,
        temperature_celcius FLOAT,
        precipitation_mm FLOAT,
        rain_mm FLOAT,
        snowfall_cm FLOAT,
        wind_speed_kmh FLOAT,
        cloud_cover INT,
        humidity INT
    );
    """  
    conn = hook.get_conn()
    cursor = conn.cursor() 
    try:
        logging.info("Checking/Creating table ...")
        cursor.execute(sql)
        conn.commit()
        logging.info("✅ Table is ready.")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to create table: {e}")
    finally:
        cursor.close()
        conn.close()


def clear_data_from_db_before_inserting(**context):
    #Clear Data from Target table to prevent inserting duplicate values
    logical_date = context['data_interval_start']
    year = logical_date.year
    month = logical_date.month
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        logging.info(f"Preparing to delete records for {year}-{month:02d}")

        sql = f"""
            DELETE FROM {SCHEMA}.{TARGET_TABLE} 
            WHERE EXTRACT(MONTH FROM date) = '{month:02d}'
            AND EXTRACT(YEAR FROM date) = '{year}';
        """
        cursor.execute(sql)

        # When using hook.get_conn() directly, Postgres does NOT auto-commit.
        # You must explicitly call commit() to persist the changes.
        conn.commit()
        
        deleted_count = cursor.rowcount
        logging.info(f"✅ Successfully deleted {deleted_count} records for {year}-{month:02d}")

    except Exception as e:
        # Roll back in case of error to unlock the table
        conn.rollback()
        raise Exception(f"Failed to delete data: {e}")
    finally:
        # 5. Explicitly close resources
        cursor.close()
        conn.close()
        logging.info("Postgres connection and cursor closed.")




def join_data_and_save_to_db(**context):
    #Download both Parquet files from S3, join them into a single dataframe, convert it to in-memory CSV, and upload to Postgres
    logical_date = context['data_interval_start']
    year = logical_date.year
    month = logical_date.month
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    GCS_PATH_1 = f"raw/accidents/monthly_summary_accidents_{year}_{month:02d}.parquet"
    GCS_PATH_2 = f"raw/weather/monthly_summary_weather_{year}_{month:02d}.parquet"
    
    # Download both datasets from gcs into a BytesIO buffer
    nyc_accidents_df = get_from_gcs_to_df(GCS_PATH_1)
    nyc_weather_df = get_from_gcs_to_df(GCS_PATH_2)
    
    # Combine
    # Ensure 'date' columns are the same type (string or datetime)
    nyc_accidents_df.rename(columns={'crash_date': 'date'}, inplace=True)
    nyc_accidents_df['date'] = pd.to_datetime(nyc_accidents_df['date'])
    nyc_weather_df['date'] = pd.to_datetime(nyc_weather_df['date'])
    df_final = pd.merge(nyc_accidents_df, nyc_weather_df, on="date", how="inner")

    # Serialize DataFrame to an in-memory CSV buffer
    csv_buffer = io.StringIO()
    # Write to CSV without index and using '|' as delimiter
    df_final.to_csv(csv_buffer, index=False, header=False, sep='|') 
    # CRITICAL: Reset the buffer pointer to the beginning for the hook to read it.
    csv_buffer.seek(0)
    
    # Define the PostgreSQL COPY command using STDIN
    copy_sql = f"""
       COPY {SCHEMA}.{TARGET_TABLE} 
       FROM STDIN 
       WITH (FORMAT CSV, DELIMITER '|', HEADER FALSE);
    """
    
    # Load data using PostgresHook's copy_expert
    logging.info(f"Loading data into PostgreSQL table: {SCHEMA}.{TARGET_TABLE}")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Call the psycopg2 cursor's copy_expert directly.
        # This is guaranteed to handle the io.StringIO object correctly.
        cursor.copy_expert(
            sql=copy_sql, 
            file=csv_buffer # psycopg2 uses the 'file' argument for streaming
        )
        
        # Commit the transaction
        conn.commit()
        logging.info(f"✅ Data transfer to PostgreSQL success for: {year}-{month:02d}")
        
    except Exception as e:
        # Ensure the connection is closed and the transaction is rolled back on error
        conn.rollback()
        raise Exception(f"Failed to execute COPY to Postgres: {e}")
        
    finally:
        # Always ensure the connection is closed
        cursor.close()
        conn.close()
        logging.info("Postgres connection and cursor closed.")


    
with DAG(
    dag_id='nyc_road_accidents_weather_correlation',
    start_date=datetime(2026, 4, 1),
    schedule_interval="25 23 7 * *", # run the job at 23:25 UTC on the 7th day of every month
    default_args=default_args,
    max_active_tasks=1,
    max_active_runs=1,
    catchup = False,
    on_success_callback=EmailNotificationLibrary.notify_success
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_accidents_and_save_to_s3 = PythonOperator(
        task_id="fetch_accidents_and_save_to_s3",
        python_callable=fetch_last_month_accidents
    )

    fetch_weather_and_save_to_s3 = PythonOperator(
        task_id="fetch_weather_and_save_to_s3",
        python_callable=fetch_last_month_weather
    )

    create_target_table = PythonOperator(
        task_id="create_target_table",
        python_callable=create_target_table_if_not_exists
    )

    clear_data_before_inserting = PythonOperator(
        task_id="clear_data_before_inserting",
        python_callable=clear_data_from_db_before_inserting
    )

    join_data_and_save_to_db = PythonOperator(
        task_id="join_data_and_save_to_db",
        python_callable=join_data_and_save_to_db
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> fetch_accidents_and_save_to_s3 >> fetch_weather_and_save_to_s3 >> create_target_table >> clear_data_before_inserting >> join_data_and_save_to_db >> end
