import datetime
# --------------- SQL CONNECTIONS ---------------- #
CONN_PARAMS = {
    "host" : "host.docker.internal",
    "user" : "root",
    "password" : "password",
}

DATE_FILENAME = "/tmp/periods.txt"
START_DATE = datetime.datetime(2020, 1, 1) # date to start the simulation
END_DATE = datetime.datetime(2024, 1, 1) # date to end the simulation

# ------ DATASET ------ #
DATA_DIR = "/opt/airflow/data"
TEMP_DIR = "/opt/airflow/tmp/"

KAGGLE_DATASETS = {
    "netflix_titles.csv" : "shivamb/netflix-shows", 
    "TMDB_movie_dataset_v11.csv" : "asaniczka/tmdb-movies-dataset-2023-930k-movies"
}

TMDB_URL = "https://api.themoviedb.org/3/discover/movie"