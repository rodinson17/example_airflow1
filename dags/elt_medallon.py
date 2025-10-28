import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from elt.ingesta import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver
from elt.dim_countries import build_dim_countries
from elt.dim_indicators import build_dim_indicators
from elt.fact_indicators import build_fact_incators

BOGOTA_TZ = pendulum.timezone("America/Bogota")
DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

# Capa raw
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "datos"
# Capa Bronze
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "datos"
#Capa Silver 
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "datos"
#Dim countries
DIM_COUNTRIES_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "countries.parquet"
#Dim indicators
DIM_INDICATORS_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "indicators.parquet"
#Dim fact indicators
FACT_INDICATORS_PATH = DATA_LAKE_ROOT / "gold" / "facts" / "fact_indicators.parquet"

# "start_date": pendulum.date(2020, 1, 1),
INGEST_PARAMS = {    
    "output_dir": str(RAW_ROOT),
    "start_year": 2015,
    "end_year" : 2024,
}

BRONZE_PARAMS = { 
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / "datos_abiertos.parquet"),
}

SILVER_PARAMS ={
    "bronze_path": str(BRONZE_PATH / "datos_abiertos.parquet"),
    "silver_path": str(SILVER_PATH / "datos_abiertos_silver.parquet"),
}

DIM_COUNTRIES_PARAMS ={
    "silver_path": str(SILVER_PATH / "datos_abiertos_silver.parquet"),
    "output_path": str(DIM_COUNTRIES_PATH),
}

DIM_INDICATORS_PARAMS ={
    "silver_path": str(SILVER_PATH / "datos_abiertos_silver.parquet"),
    "output_path": str(DIM_INDICATORS_PATH),
}

FACT_INDICATORS_PARAMS ={
    "silver_path": str(SILVER_PATH / "datos_abiertos_silver.parquet"),
    "output_path": str(FACT_INDICATORS_PATH),
}


with DAG(
    dag_id="elt_medallon",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 10, 23, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "api"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingesta",
        python_callable=ingest_to_raw,
        op_kwargs=INGEST_PARAMS,
    )

    bronze_task = PythonOperator(
        task_id="copy_to_bronze",
        python_callable=copy_raw_to_bronze,
        op_kwargs=BRONZE_PARAMS,
    )

    silver_task = PythonOperator(
        task_id="to_silver",
        python_callable=transform_bronze_to_silver,
        op_kwargs=SILVER_PARAMS,
    )

    countries_task = PythonOperator(
        task_id="dim_countries",
        python_callable=build_dim_countries,
        op_kwargs=DIM_COUNTRIES_PARAMS,
    )

    indicators_task = PythonOperator(
        task_id="dim_indicators",
        python_callable=build_dim_indicators,
        op_kwargs=DIM_INDICATORS_PARAMS,
    )

    fact_indicators_task = PythonOperator(
        task_id="fact_indicators",
        python_callable=build_fact_incators,
        op_kwargs=FACT_INDICATORS_PARAMS,
    )

    ingest_task >> bronze_task >> silver_task >> [countries_task, indicators_task] >> fact_indicators_task