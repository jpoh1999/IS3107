from airflow.decorators import task, dag, task_group
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from constants import *

from helpers.dataengineer import ingest
from helpers.dummyops import start, end, datawarehouse_etl_start, await_tasks

from helpers.ml import etl_ml;
from dags.helpers.operations import etl_dashboard;

from database.warehouse import *
from database.lake import *


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024, 1, 1),
    "catchup": False
}


@dag(
    max_active_runs=8, # backfill workers
    schedule_interval=None,
    catchup=False,
    tags=["is3107", "batch-processing"]
)

def batch_etl() :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Batch etl for the movies analytic company
    """
    @task_group(group_id = "data_engineers")
    def data_engineer() :
        """
            Task group for data engineers
            It does the following :
            a. Group ingestion for the different data sources
            b. Download files parallelly to save time
        """
        ingests = []
        
        for table_name, meta_data in MOVIES_META.items():
            # folder = datetime.datetime.now().strftime("%Y%m%dT%H%M%S") # use the current datetime as folder
            group_id=f"individual_ingest_{table_name}"
            par_dir = f"{TEMP_DIR}/{table_name}" # parent directory
            individual_ingest = ingest.override(group_id=group_id) (
                table_name=table_name,
                meta_data=meta_data,
                par_dir=par_dir,
                bucket_name=BUCKETNAME
            )                                       
            
            ingests.append(individual_ingest) # download files into temp directory if not exists
        
        return start() >> ingests >> end()

    @task_group(group_id="operations")
    def operations() :
        """
            ETL the relevant data from the datawarehouse into ops database
            And perform some other tasks for ops
            TODO: Implement logic for connecting to dashboard, etc.
        """
        start() >> etl_dashboard() >> await_tasks() >> end()

    @task_group(group_id = "machine_learnig")
    def machine_learnig() :
        """
            ETL the relevant data from the datawarehouse into ml database
            And perform some other tasks for ml
            TODO: Implement logic for mltraining and deployment
        """
        start() >> etl_ml() >> await_tasks() >> end()


    data_engineer() >> datawarehouse_etl_start() >> [operations(), machine_learnig()]



batch_etl()        

