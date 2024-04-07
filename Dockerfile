FROM apache/airflow:2.8.4
ADD requirements.txt .
RUN pip install -r requirements.txt
