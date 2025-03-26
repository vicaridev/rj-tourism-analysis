FROM python:3.9-slim

WORKDIR /tourism_analysis

COPY etl/ etl/
COPY services/ services/
COPY config/ config/
COPY data/ data/
COPY requirements.txt .



RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x /tourism_analysis/etl/run_etl.sh

CMD ["/tourism_analysis/etl/run_etl.sh" ]