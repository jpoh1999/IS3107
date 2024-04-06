DASHBOARDMART_PARAMS = {
    "host" : "host.docker.internal",
    "user" : "root",
    "password" : "password",
    "database" : "dashboard"
}

MLMART_PARAMS = {
    "host" : "host.docker.internal",
    "user" : "root",
    "password" : "password",
    "database" : "machinelearning"
}

DBWAREHOUSE_PARAMS = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "password",
    "database": "datawarehouse",
}
DATABASES = [DASHBOARDMART_PARAMS, MLMART_PARAMS, DBWAREHOUSE_PARAMS]
# ----- GCS CONFIGURATIONS ------ #
BUCKETNAME = "is3107_datalake"
PROJECT_ID = "bt4301-g16"
CREDENTIALS = "credentials.json"