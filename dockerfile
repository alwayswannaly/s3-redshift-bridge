FROM python:3.9-alpine
WORKDIR /app

COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt
