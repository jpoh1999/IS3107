from airflow.decorators import task, dag, task_group
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from constants import *
from helpers.ingest import download_from_kaggle
from database.warehouse import *
from database.lake import *

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024, 1, 1),
    "catchup": False
}


@dag(
    max_active_runs=8, # parallel workers to speed up batch ingestion
    schedule_interval=None,
    catchup=False,
    tags=["is3107", "batch-processing"]
)

def batch_etl() :
    """
        Batch etl
    """
    @task(task_id="start")
    def start() :
        """
        Dummy operator
        """
        pass

    

    @task_group(group_id = "ingest")
    def batch_ingest() :
        """
            Download files parallelly to save time
        """
        ingests = []
        
        for table_name, meta_data in MOVIES_META.items():
            # folder = datetime.datetime.now().strftime("%Y%m%dT%H%M%S") # use the current datetime as folder
            group_id=f"individual_ingest_{table_name}"
            
            @task_group(group_id = group_id)
            def ingest(table_name : str, meta_data : dict) :
                conn = meta_data["connection"] # connection 
                file_name = meta_data["file_name"] # filename
                dataset_name = meta_data["dataset_name"] # datasetname
                columns = meta_data["columns"] # columns
                column_mapping = meta_data["column_mapping"] # mappings
                par_dir = f"{TEMP_DIR}/{table_name}" # parent directory
                src_file_path = f"{par_dir}/{file_name}" # src_file after concatenating with parent directory
                dest_file_path = f"data/{file_name}" # destination path

                download = download_from_kaggle.override(task_id=f"download_files_{table_name}") \
                                        (dataset_name, par_dir)
                upload = PythonOperator(task_id =f"uploading{table_name}", 
                                        python_callable = upload_blob, 
                                        op_args = [BUCKETNAME, 
                                                    src_file_path, 
                                                    dest_file_path]) # store in gcs
                load = PythonOperator(task_id =f"loading{table_name}", 
                                        python_callable = load_from_csv_batch_sql, 
                                        op_args = [
                                                conn,
                                                src_file_path,
                                                table_name,
                                                columns,
                                                column_mapping]) # store in gcsload_from_csv_batch_sql(conn_params=conn,
                start() >> download >> [upload,load] >> end()
            
            individual_ingest = ingest(table_name=table_name,
                                       meta_data=meta_data)                                        
            
            ingests.append(individual_ingest) # download files into temp directory if not exists
        
        return start() >> ingests >> end()

     
    @task(task_id = "end", trigger_rule="all_success")
    def end() :
        """
        Dummy operator
        """
        pass

    start() >> batch_ingest() >> end()


batch_etl()        

