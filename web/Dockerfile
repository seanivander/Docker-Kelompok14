FROM python:3.9

WORKDIR /app

COPY ./requirements.txt .
COPY ./data_ingestion_script.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "data_ingestion_script.py"]