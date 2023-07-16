FROM python:3.11.4-bookworm

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

