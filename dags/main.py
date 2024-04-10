from airflow.decorators import task, dag, task_group
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}


@dag(
    max_active_runs=1, # prevent multi dags
    # schedule_interval=datetime.time_delta(minutes=5),
    catchup=False,
    tags=["is3107"]
)

def realtime_etl_pipeline() :
    """
        To simulate a realtime etl
        We will use MySql as the datawarehouse and datamarts 
        And GCS as the backup data lake
        After ingesting all data into the datawarehouse,
        we ingest the raw data into a 2nd layer, the gcs,
        Then we will execute some transformation and load the new data 
        into the respective datamarts
        - Dashboard 
        - ML
    """

    """
        Api ?
    """


