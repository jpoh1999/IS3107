from airflow.decorators import task
import logging

"""
    This file contains all dummy helper operators for
    orchestrating airflow structure
"""

@task(task_id="start")
def start(**context):
    """
        Dummy operator for start

    """
    pass

@task(task_id = "debug")
def debug(item) :
    """
        Debugger operator for task_groups
    """
    logging.info(item)

@task(task_id = "end", trigger_rule="all_success")
def end() :
    """
        Dummy Operator for end
    """
    pass

@task(task_id = "await")
def await_tasks():
    """
        Dummy Operator for awaiting
    """
    pass

@task(task_id = "datawarehouse_etl_start")
def datawarehouse_etl_start() :
    """
        Dummy Operator for Datawarehouse staging
    """

    pass
