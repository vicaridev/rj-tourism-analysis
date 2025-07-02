FROM apache/airflow:2.10.5-python3.10

USER root

COPY . /app
WORKDIR /app

RUN chown -R airflow: /app

USER airflow

RUN pip install --upgrade pip
RUN pip install .