
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
