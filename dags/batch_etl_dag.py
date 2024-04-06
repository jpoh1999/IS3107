from airflow.decorators import task, dag, task_group
from datetime import datetime
from  constants import *
from helpers.ingest import download_from_kaggle
from database.warehouse import *
from database.lake import *

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}


@dag(
    max_active_runs=8, # parallel workers to speed up batch ingestion
    schedule_interval=None,
    catchup=False,
    tags=["is3107"]
)

def batch_etl() :
    """
        Batch etl
    """

    @task_group(group_id = "ingest")
    def batch_download(**context) :

        for filename, dataset_name in KAGGLE_DATASETS.items():
            download_from_kaggle(dataset_name, TEMP_DIR) # download files into temp directory

            src_file_path = f"{TEMP_DIR}/{filename}"
            dest_file_path = f"kaggle_data/{filename}"

            upload_blob(BUCKETNAME, src_file_path, dest_file_path) # store in gcs

            for 

    

        

