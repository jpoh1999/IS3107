from airflow.decorators import task, dag, task_group
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from constants import *

from helpers.dataengineer import ingest
from helpers.dummyops import start, end, datawarehouse, await_tasks

from helpers.ml import etl_ml, data_preparation, ml_ops
from helpers.operations import etl_dashboard;

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
        
        Co-authors : Wei Han, Jiayi, Shan Yi, Mei Lin

        Tag: Batch etl for the movies analytic company

        Description : As film production is costly, 
        it is crucial to create a movie that appeals to the majority of the audience 
        so that the film production company can maximize their profits. 
        Therefore, the film industry should not only innovate in terms of content and technology 
        but also create movies that best cater to the increasingly picky audience's taste. 
        Consequently, we would like to analyze and understand patterns in the movie industry over time. 
        This analysis will empower industry stakeholders to make informed decisions on future productions, 
        marketing strategies, and content curation. 
    """
    @task_group(group_id = "data_engineers")
    def data_engineer() :
        """
            Task group for data engineers

            It does the following :
            a. Group ingestion for the different data sources
            b. Download files parallelly to save time
            c.
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

    @task_group(group_id = "machine_learning")
    def machine_learning() :
        """
            ETL the relevant data from the datawarehouse into ml database
            And perform some other tasks for ml
            TODO: Implement logic for mltraining and deployment
        """
        start() >> etl_ml() >> data_preparation() >> ml_ops() >> end()


    data_engineer() >> datawarehouse() >> [operations(), machine_learning()]



batch_etl()        

