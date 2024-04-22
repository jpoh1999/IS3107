from airflow.decorators import task
import datetime
import logging
import pandas as pd
import mysql.connector
from dateutil.relativedelta import relativedelta
from typing import Callable, Any
from mysql.connector import Error

def set_up_global_infile(db_params : dict) :
    """
    Set up local_infile to load to big data into sql server

    Args : 
        - db_params : (dict) db params
    
    Expected Outcome :
        - local_infile set up for database
    """
    set_up_global_infile = "SET GLOBAL local_infile = true;"

    my_sql_connector_query(conn_params=db_params, query_list=[set_up_global_infile])


def create_drop_new_database(db_params: dict, create_or_drop : str) :
    """
    Create a new database based on database parameters

    Args :
        - db_params : (dict) database parameters
    
    Expected Outcome :
        - A new database is created
    """
    conn_params = db_params.copy()
    database_name = conn_params.pop("database")
    conn = mysql.connector.connect(
        **conn_params
    )
    # get the connection cursor
    cursor = conn.cursor()
   
    try:  
        #creating or dropping a new database
        if create_or_drop == "CREATE":
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        else:
            cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")

    except Error as err:
        print(f"Error: '{err}'")
 
    cursor.close()
    conn.close()

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
        logging.info(conn_params)
        
        db = mysql.connector.connect(
            **conn_params
        )  # Connect one time to DBWAREHOUSE using mysql connector

        cursor = db.cursor()  # Create a cursor

        # -------------------- EXECUTE QUERIES --------------------- #
        for query in query_list:
            logging.info(query)
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


def drop_create_tables(conn_params: dict, create_queries : list, drop_queries : list):
    """
    Drop tables if exists in database using mysql.connector
    Precondition : database is already created in the host machine
    Args :
        - conn_params : (dict) connection parameters to database
        - create_queries : (list) list of create queries with the schemas defined
        - drop_queries : (list) list of drop queries
    Expected outcome :
        - Drop tables {...}
          if created in database
        - Create tables {...} after
    """
    
    
    my_sql_connector_query(conn_params, drop_queries)
    my_sql_connector_query(conn_params, create_queries)

    return

def load_from_csv_batch_sql(conn_params : dict, file_name : str, table_name : str, columns : list, column_mapping : dict):
    """
        Generate the load queries sql for each table dynamically

        Args :
            - conn_params : (dict) connection parameters to database
            - file_name : (str) the name of the file
            - table_name : (str) name of the table 
            - columns : (list) the columns of the csv file
            - column_mapping : (dict) the columns to keep in the sql database
    """

    # Generate the fields part of the query
    fields_part = ', '.join([f'@{column}' for column in columns])

    # Remove last
    
    # Generate the SET part of the query
    set_part = ', '.join([f'{column}={column_mapping[column]}' for column in column_mapping.keys()])

    logging.info(set_part)
    # Generate the full query
    load_query = f"""
        LOAD DATA LOCAL INFILE '{file_name}'
        INTO TABLE 
            {table_name}
        FIELDS TERMINATED BY ","  -- Adjust based on your file format
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY "\\n"  -- Adjust line terminator if needed
        IGNORE 1 LINES
        ({fields_part}) 
        SET 
            {set_part}
        ;
    """
    logging.info(load_query)
    
    my_sql_connector_query(conn_params=conn_params, query_list=[load_query])

    return