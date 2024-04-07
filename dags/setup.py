from airflow.decorators import dag, task, task_group
from database.warehouse import *
from database.lake import *
from database.sql import *
from datetime import datetime
from constants import BUCKETNAME

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}
@dag(
    max_active_runs=1,  # prevent multiple runs
    schedule_interval=None,  # timedelta(minutes=1),
    catchup=False,
    tags=["bt4301-a1"],
)
def setup():
    @task(task_id="start")
    def start():
        pass

    @task(task_id="set_up_datawarehouse")
    def set_up_databases():
        """
        Set up databases to ingest all the movie data
        """
        for database in DATABASES :
            create_drop_new_database(database, "CREATE")

            drop_create_tables(database, 
                               create_queries=CREATE_QUERIES,
                               drop_queries=DROP_QUERIES)
            
            set_up_global_infile(database)
    
    @task(task_id="set_up_datalake")
    def set_up_datalake():
        """
        Set up datalake in gcs to backup files
        """
        try :
            create_bucket(BUCKETNAME)
        except Exception as e:
            logging.error("BUCKET ALREADY CREATE!! SKIPPING-")
    
            
    @task(task_id="end")
    def end():
        pass

    start() >> set_up_databases() >> set_up_datalake() >> end()


setup()
