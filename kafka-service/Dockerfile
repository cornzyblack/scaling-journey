# FROM python:3.10
FROM python:3.10.8-slim-buster

WORKDIR /app

COPY . .
RUN ls
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

CMD ["python3", "producer.py"]
