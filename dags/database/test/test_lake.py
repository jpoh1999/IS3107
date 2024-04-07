import unittest
import json
from database.constants import *
from database.lake import *
import logging

BUCKETNAME = "test"
TEST_FILE_NAME = "dags/database/test/test.txt"
DESTINATION_FILE_NAME = "test.txt"

class TestStoreMethods(unittest.TestCase):
    """
    This is a unit test chunk to test the following functions
    a. Create Bucket
    b. Load Json Files into Lake - Google Cloud Storage
    c. 
    """
    # TODO: Create test case to test create_bucket method
    def test_create_bucket(self):
        try :
            create_bucket(BUCKETNAME)
        except Exception as e:
            logging.error("Failed to create a bucket")

    # TODO: Create test case to test upload_blob method
    def test_upload_blob(self):
        
        upload_blob(BUCKETNAME, TEST_FILE_NAME, DESTINATION_FILE_NAME)
        
        # self.assertEqual()

    # TODO: Create test case to test get_blob method
    def test_get_blob(self):
        pass

    def test_dag() :

        # table_name = "movies_casts"
        # meta_data = MOVIES_META[table_name]
        # conn = meta_data["connection"]
        # file_name = meta_data["file_name"] 
        # dataset_name = meta_data["dataset_name"]
        # columns = meta_data["columns"] 
        # column_mapping = meta_data["column_mapping"]
        
        # file_name = "netflix_titles.csv"
        # src_file_path = f"{TEMP_DIR}/{file_name}"
        # dest_file_path = f"data/{file_name}"

        # download = download_from_kaggle(dataset_name, TEMP_DIR)
        # upload = PythonOperator(task_id =f"uploading{table_name}", 
        #                                     python_callable = upload_blob, 
        #                                     op_args = [BUCKETNAME, 
        #                                             src_file_path, 
        #                                             dest_file_path]) # store in gcs
        # load = PythonOperator(task_id =f"loading{table_name}", 
        #                                 python_callable = load_from_csv_batch_sql, 
        #                                 op_args = [
        #                                             conn,
        #                                             file_name,
        #                                             table_name,
        #                                             columns,
        #                                             column_mapping]) #