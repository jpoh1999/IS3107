from airflow.decorators import task
import datetime
import logging
import pandas as pd
import mysql.connector
from dateutil.relativedelta import relativedelta
from typing import Callable, Any

def create_drop_new_database(db_params: dict, query : str) :
    """
    Create a new database based on database parameters

    Args :
        - db_params : (dict) database parameters
    
    Expected Outcome :
        - A new database is created
    """
    database_name = db_params.pop("database")

    conn = mysql.connector.connect(
        **db_params
    )
    
    # get the connection cursor
    cursor = conn.cursor()

    
    try:  
        #creating or dropping a new database  
        cursor.execute(f"{query} database {database_name}")  
    
        #getting the list of all the databases which will now include the new database PythonDB  
        cursor.execute("show databases")  

      
    except:  
        logging.error("Error while creating/dropping databases!")
        # rollback changes on error
        conn.rollback()  

def my_sql_connector_query(conn_params: dict, query_list: list):
    """
    Generalized method to send queries to connection using mysql.connector
    Precondition :

    conn_params is a valid connection parameters
    query_list is a list of valid sql queries

    Args:
        - conn_params : (dict) connection parameters
        - query_list : (list) list of SQL queries

    Expected outcome :
        - Execute queries into database based on connection parameters

    """
    try:
        db = mysql.connector.connect(
            **conn_params
        )  # Connect one time to DBWAREHOUSE using mysql connector

        cursor = db.cursor()  # Create a cursor

        # -------------------- EXECUTE QUERIES --------------------- #
        for query in query_list:
            cursor.execute(query)

        # ----------- COMMIT CHANGES AND CLOSE CONNECTION ---------- #
        db.commit()
        db.close()

    except Exception as e:
        # Log the error message if found
        logging.error(e)
    else:
        # Send a success message if no errors was found
        logging.info("SUCCESSFULLY EXECUTED QUERIES")


def drop_create_tables(conn_params: dict, drop_queries: list, create_queries : list, **context):
    """
    Drop tables if exists in DBWAREHOUSE using mysql.connector
    Precondition : DBWAREHOUSE SCHEMA is already created in the host machine
    Args :
        - conn_params : (dict) connection parameters
        - drop_queries : (list) list of queries to drop tables
    Expected outcome :
        - Drop tables {SALES, STORE, PRODUCT, TIME, FREIGHT}
          if created in DBWAREHOUSE
    """

    ti = context["ti"]
    start_date = ti.xcom_pull(task_ids="start", key="dag_date")
    first_record = ti.xcom_pull(task_ids="start", key="first_record")

    if first_record:
        logging.info(f"DROPPING TABLES... ON {start_date}")
        my_sql_connector_query(conn_params, drop_queries)
        my_sql_connector_query(conn_params, create_queries)

    return