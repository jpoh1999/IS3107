from airflow.decorators import dag, task, task_group
from constants import DATE_FILENAME, DATABASES
from database.warehouse import *
import os

"""
    This dag is used to teardown the workflow
    
"""
@dag(
    max_active_runs=1,  # prevent multiple runs
    schedule_interval= None,#timedelta(minutes=1),
    catchup=False,
    tags=["is3107", "teardown"],
)
def teardown() :

    @task(task_id="delete_file")
    def delete_file(file_name):

        """
            Author : James Poh Hao
            Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

            Description : Delete all file remnants related to the Workflow
        """
        try:
            os.remove(file_name)
            print(f"File '{file_name}' has been successfully deleted.")
        except FileNotFoundError:
            print(f"File '{file_name}' not found.")
        except Exception as e:
            print(f"An error occurred while deleting the file '{file_name}': {e}")

    @task(task_id="delete_databases")
    def delete_databases() :
        """
            Author : James Poh Hao
            Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

            Description : Delete databases to free up server storage
        """
        for database in DATABASES:
            create_drop_new_database(database, "DROP")


    delete_file(DATE_FILENAME) >> delete_databases()


teardown()
