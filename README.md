
# Set-up
```bash
git clone https://github.com/jpoh1999/IS3107/

cd IS3107

docker build -t "is3107:1.0" .
docker compose up airflow-init

docker compose up -d
```

# Teardown
```bash
docker compose down -v
```

# Dag Structure

# CurrentDagStructure
The picture below shows the current dag structure
<img width="1481" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/9980d894-c8f1-4cbb-b76e-a5649ef7192b">


# Schemas of Tables in Datawarehouse :

## Table : movie ratings
<img width="1224" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/a3a4a552-48cf-4074-8ddf-a82bb1b8bb35">

## Table : movie casts

<img width="1293" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/9dd8373c-9102-4056-ac25-fe4ee7e9b0bf">

## Table : movie finance

<img width="560" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/35d9e937-b38d-4661-867a-f11fe9c76606">
# Data Lake For Backups :

<img width="919" alt="image" src="https://github.com/jpoh1999/IS3107/assets/157945682/e3ed10df-2389-4079-bc9e-5682b80cda04">
