import datetime
# --------- SQL CONFIGURATIONS ----------- #
DASHBOARDMART_PARAMS = {
    "host" : "host.docker.internal",
    "user" : "root",
    "password" : "20021202",
    "database" : "dashboard",
    "allow_local_infile" : True,
}

MLMART_PARAMS = {
    "host" : "host.docker.internal",
    "user" : "root",
    "password" : "20021202",
    "database" : "machinelearning",
}

DBWAREHOUSE_PARAMS = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "20021202",
    "database": "datawarehouse",
    "allow_local_infile" : True,
}

DATABASES = [DASHBOARDMART_PARAMS, MLMART_PARAMS, DBWAREHOUSE_PARAMS]

# ----- GCS CONFIGURATIONS ------ #
BUCKETNAME = "is3107_datalake"
PROJECT_ID = "bt4301-g16"
CREDENTIALS = "/home/airflow/.google/credentials.json"



# ------ OS PATHS ------ #
DATA_DIR = "/opt/airflow/data"
TEMP_DIR = "/opt/airflow/tmp"

# ------ METADATA FOR DATASETS ------ #
# metadata e.g : 
# name_table_in_sql : {
#     connection : conenction to the database where table is stored
#     file_name : the name of the file,
#     dataset_name : the dataset name for kaggle/api extraction 
#     url : the endpoint url of the api 
#     columns : the columns of the ingested file
#     column_maping : mapping of the required/transformed cols
# }
MOVIES_META = { 
    "movies_casts" : {
        'connection' : DBWAREHOUSE_PARAMS,
        'file_name' : "netflix_titles.csv",
        'dataset_name' : 'shivamb/netflix-shows',
        'url' : None,
        'columns' : ['id', 'type', 'title', 'director', 'cast', 'country', 'date_added', 
           'release_year', 'rating', 'duration', 'listed_in', 'description'],
        'column_mapping' : {
            'title' : '@title', # primary key
            'type' : '@type',
            'director': '@director',
            'cast': '@cast',
            'country': '@country',
            'date_added': "STR_TO_DATE(@date_added,'%M %d, %Y')",
            'release_year': '@release_year',
        }
    },
    "movies_rating" : {
        'connection' : DBWAREHOUSE_PARAMS,
        'file_name' : "TMDB_movie_dataset_v11.csv",
        'dataset_name' : 'asaniczka/tmdb-movies-dataset-2023-930k-movies',
        'url' : None,
        'columns' : ['id', 'title', 'vote_average', 'vote_count', 'status', 'release_date',
                    'revenue', 'runtime', 'adult', 'backdrop_path', 'budget', 'homepage',
                    'imdb_id', 'original_language', 'original_title', 'overview',
                    'popularity', 'poster_path', 'tagline', 'genres',
                    'production_companies', 'production_countries', 'spoken_languages',
                    'keywords'],
        'column_mapping' : {
            'title': '@original_title', # primary key
            'show_rating': '@vote_average',
            'popularity' : '@popularity',
            'total_votes' : '@vote_count',
            'release_date': '@release_date',
            'genres' : '@genres',
            'description' : '@overview',
            'languages' : '@spoken_languages',
            'keywords' : '@keywords'
        }
    },

    "movies_finance" : {
        'connection' : DBWAREHOUSE_PARAMS,
        'file_name' : "TMDB_movie_dataset_v11.csv",
        'dataset_name' : 'asaniczka/tmdb-movies-dataset-2023-930k-movies',
        'url' : None,
        'columns' : ['id', 'title', 'vote_average', 'vote_count', 'status', 'release_date',
                    'revenue', 'runtime', 'adult', 'backdrop_path', 'budget', 'homepage',
                    'imdb_id', 'original_language', 'original_title', 'overview',
                    'popularity', 'poster_path', 'tagline', 'genres',
                    'production_companies', 'production_countries', 'spoken_languages',
                    'keywords'],
        'column_mapping' : {
            'title': '@original_title', # primary key
            'revenue': '@revenue',
            'budget': '@budget',
            'production_companies': '@production_companies',
            'production_countries': '@production_countries',
        }
    }
}

# --------------- OTHERS ---------------- #


DATE_FILENAME = "/tmp/periods.txt"
START_DATE = datetime.datetime(2020, 1, 1) # date to start the simulation
END_DATE = datetime.datetime(2024, 1, 1) # date to end the simulation