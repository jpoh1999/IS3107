from airflow.decorators import dag, task, task_group
from database.constants import *
from database.warehouse import *
from database.lake import *

from constants import BUCKETNAME
@dag(
    max_active_runs=1,  # prevent multiple runs
    schedule_interval=None,  # timedelta(minutes=1),
    catchup=False,
    tags=["bt4301-a1"],
)
def sql_migration_dag():
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
    
    @task(task_id="set_up_datalake")
    def set_up_datalake():
        """
        Set up datalake in gcs to backup files
        """
        create_bucket(BUCKETNAME)
            
    @task(task_id="end")
    def end():
        pass

    start() >> set_up_databases() >> set_up_datalake() >> end()


sql_migration_pipeline = sql_migration_dag()
