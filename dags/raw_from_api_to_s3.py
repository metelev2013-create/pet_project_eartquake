# Импорты:
import duckdb
import pendulum
from airflow import DAG
from airflow.modes import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythoonOperator

# Кофигурация DAG
OWNER = "v.metelev"
DAG_ID = "raw_from_api_to_s3"
#  Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
#S3
accec="Qk62bGohhxC6YF502L3Y"
secret="a0ugd5fADZVz5u6nDGpViMjza6wbMfdOxi4qrAnV"
# LONG DESCRIPTION
"""
SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year: 2025, month:5, day:10)
    "catchup":3,
    retray_delay": pendulim.duration(hors=1),
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def get_and_transfer_api_data_to_s3(**context):
    """"""
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end