
# Set-up
```bash
git clone https://github.com/jpoh1999/IS3107/

cd IS3107

docker build -t "is3107:1.0" . -f Dockerfile.airflow
docker compose up airflow-init

docker compose up -d
```

# Teardown
```bash
docker compose down -v
```

# Dag Structure
Run in this sequence : setup >> batch_pipeline >> teardown
<img width="1655" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/bd713ed6-aab2-4ffe-8dab-5646ac51d461">

# CurrentDagStructure
The picture below shows the current dag structure

<img width="1109" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/ec66fd9f-3fdc-4c4d-a99a-beb7febf46b6">

<img width="1109" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/c22dd6c0-f810-4200-ac89-f44debe9c264">

OperationsWorkflow :
<img width="443" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/b6f8991e-dc5b-425f-8941-70bbbcb258df">

MachineLearningWorkflow :
<img width="978" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/10de201a-0adb-47c8-bbd7-9781b2025d7c">


# Schemas of Tables in Datawarehouse :

## Table : movie ratings
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/a3a4a552-48cf-4074-8ddf-a82bb1b8bb35">

## Table : movie casts

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/9dd8373c-9102-4056-ac25-fe4ee7e9b0bf">

## Table : movie finance

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/35d9e937-b38d-4661-867a-f11fe9c76606">

# Schemas of Tables in Dashboard Mart :

## Table : Actors 
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/be2656b3-9156-465d-9e6b-c22a4cb86e95">

## Table : Genres
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/4f6eee6b-3521-440e-b457-9918246ddad1">

## Table : Movies
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/385bb66e-f54c-47db-90ee-12a707456b03">

# Schema of Tables in Machine Learning Mart :

## Table : Movies
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/d4e6906a-df8c-4a51-86e6-714860dc4b1b">

# MLFlow Artifact Repository

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/342794a6-f187-471e-8507-2865a45e7b9e">

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/9d7b9e69-605c-4f29-a11e-c89743bd5e3a">

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/a9233df3-7c38-4df0-bd78-d02c97547693">


# Data Lake For Backups :

<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/e3ed10df-2389-4079-bc9e-5682b80cda04">
