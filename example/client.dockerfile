FROM python:3.10-bullseye

WORKDIR /app/

RUN pip install streamback==0.0.17

CMD ["python", "example/client.py"]