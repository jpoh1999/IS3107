from airflow.decorators import dag, task, task_group
from database.warehouse import *
from database.lake import *
from database.sql import *
from datetime import datetime
from helpers.dummyops import start, end
from constants import BUCKETNAME

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

"""
    This dag is used to set-up the required components for the workflow.
"""
@dag(
    max_active_runs=1,  # prevent multiple runs
    schedule_interval=None,  # timedelta(minutes=1),
    catchup=False,
    tags=["is3107-a1","setup"],
)
def setup():

    @task(task_id="set_up_datawarehouse")
    def set_up_databases():
        """
            Author : James Poh Hao
            Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
            
            Description : Set up databases to ingest all the movie data
        """
        for database in DATABASES :
            create_drop_new_database(database, "CREATE")

            
            set_up_global_infile(database)
    
    @task(task_id="drop_create_tables_datawarehouse")
    def drop_create_tables_dw() :
        """
            Author : James Poh Hao
            Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
            
            Description : Set up the tables for datawarehouse
        """
        drop_create_tables(DBWAREHOUSE_PARAMS, create_queries=CREATE_QUERIES_DW, drop_queries=DROP_QUERIES_DW)

    @task(task_id="set_up_datalake")
    def set_up_datalake():
        """
            Author : James Poh Hao
            Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
            
            Description : Set up datalake in gcs to backup files
        """
        try :
            create_bucket(BUCKETNAME)
        except Exception as e:
            logging.error("BUCKET ALREADY CREATE!! SKIPPING-")
    

    start() >> set_up_databases() >> set_up_datalake() >> drop_create_tables_dw() >> end()


setup()
