FROM python:3.10-bullseye

WORKDIR /app/

RUN pip install confluent-kafka redis

CMD ["python", "example/client.py"]