from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.decorators import task, task_group
import os
from database.warehouse import *
from database.lake import *
from helpers.dummyops import start, end

@task(task_id = "download_file")
def download_from_kaggle(dataset: str, dirpath : str) :
    """
    Author : James Poh Hao
    Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

    Description : Download the kaggle dataset into our local directory
    :dataset: the dataset to download from kaggleapi
    :dirpath: the local dirpath to save the dataset
    """
    api = KaggleApi()
    api.authenticate() # authenticate to kaggle api

    os.makedirs(dirpath, exist_ok = True) # create a local directory if it doesnt exist

    api.dataset_download_files(dataset, path = dirpath, unzip = True, quiet = True) # download dataset into DATAPATH


# TODO : KIV
def download_from_tmdb():
    pass



@task(task_id = "upload_blob_to_gcs")
def upload_blob_task(bucket_name : str, src_file_path : str, dest_file_path : str) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        Description : Airflow Decorator task for uploading blob

        :bucket_name: the name of the bucket to upload the blob to
        :src_file_path: the path to get the file from
        :dest_file_path: the filepath in gcs
    """
    
    upload_blob(bucket_name=bucket_name, source_file_name=src_file_path, destination_blob_name=dest_file_path)
    logging.info(f"Successfully uploaded blob to {bucket_name}!" )

@task(task_id = "load_to_sql")
def upload_to_sql(conn : dict=None, src_file_path : str=None, table_name : str=None, columns : list=None, column_mapping : dict=None) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        Description : Airflow Decorator task for uploading to sql

        :conn: the connection parameters 
        :src_file_path: the filepath of the data
        :table_name: the name of the table for the data
        :columns: the columns from the raw data
        :column_mapping: the mapping of the required data from raw data
    """
    
    load_from_csv_batch_sql(conn_params=conn, file_name=src_file_path, table_name=table_name,columns=columns, column_mapping=column_mapping)
    logging.info(f"Successfully loaded to Sql {table_name}")

@task_group(group_id = "Ingest_group")
def ingest(table_name : str, meta_data : dict, par_dir : str, bucket_name : str) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        Description : Download and upload data into sql and gcs 
        for each data source

        Preconditions : 
        - We assume that the required information for the data source
        are available in the meta_data. 
        - We also assumed that there are no duplicate names for the table

        :table_name: the name of the table in the dw
        :meta_data: contains information like lineage, schema of the data,
        :par_dir: the parent dir for the src file
        :bucket_name: the gcs bucketname to store the files


    """
    conn = meta_data["connection"] # connection 
    file_name = meta_data["file_name"] # filename
    dataset_name = meta_data["dataset_name"] # datasetname
    columns = meta_data["columns"] # columns
    column_mapping = meta_data["column_mapping"] # mappings
    
    src_file_path = f"{par_dir}/{file_name}" # src_file after concatenating with parent directory
    dest_file_path = f"data/{file_name}" # destination path

    ## Download the relevant dataset from kaggle
    download = download_from_kaggle.override(task_id=f"download_files_{table_name}") (
                            dataset=dataset_name, 
                            dirpath=par_dir
                            )
                        
    ## Upload a blob of this raw data into gcs
    upload = upload_blob_task.override(task_id =f"uploading{table_name}") (
                            bucket_name=bucket_name, 
                            src_file_path=src_file_path, 
                            dest_file_path=dest_file_path
                            ) # store in gcs

    ## Load the required transformed data into warehouse
    load = upload_to_sql.override(task_id =f"uploading{table_name}") (
                                    conn=conn,
                                    src_file_path=src_file_path,
                                    table_name=table_name,
                                    columns=columns,
                                    column_mapping=column_mapping) # store in sql
    
    start() >> download >> [upload,load] >> end()