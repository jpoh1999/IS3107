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