FROM python:3.10-bullseye

COPY . /app/

WORKDIR /app/

RUN pip install .

CMD ["python", "example/client.py"]