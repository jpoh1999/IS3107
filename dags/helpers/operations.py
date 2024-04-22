from database.warehouse import *
from database.lake import *
from helpers.dummyops import start, end
from airflow.decorators import task, task_group
from database.sql import CREATE_QUERIES_DB, DROP_QUERIES_DB

@task(task_id = "etl_dashboard")
def etl_dashboard() :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        
        Description : ETL Process for Dashboard DataMart
        Entire extraction and transformation done 
        using SQL Queries.
    """

    
    drop_create_tables(DASHBOARDMART_PARAMS, CREATE_QUERIES_DB, DROP_QUERIES_DB)


"""
    Implement other tasks for ops below
"""

    
    

