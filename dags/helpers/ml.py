from database.warehouse import *
from database.lake import *
from helpers.dummyops import start, end
from airflow.decorators import task, task_group
from database.sql import CREATE_QUERIES_ML, DROP_QUERIES_ML

@task(task_id = "etl_dashboard")
def etl_ml() :
    """
        ETL Process for Dashboard DataMart
        Entire extraction and transformation done 
        using SQL Queries.
    """
    drop_create_tables(MLMART_PARAMS, CREATE_QUERIES_ML, DROP_QUERIES_ML)


"""
    Implement other tasks for ml below
"""
    
    

