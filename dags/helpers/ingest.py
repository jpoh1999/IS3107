from kaggle.api.kaggle_api_extended import KaggleApi
import os

def download_from_kaggle(dataset: str, dirpath : str) :
    """
    Download the kaggle dataset into our local directory
    """
    api = KaggleApi()
    api.authenticate() # authenticate to kaggle api

    os.makedirs(dirpath, exist_ok = True) # create a local directory if it doesnt exist

    api.dataset_download_files(dataset, path = dirpath, unzip = True, quiet = True) # download dataset into DATAPATH

# TODO : KIV
def download_from_tmdb():
    pass
