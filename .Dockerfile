FROM python:3.8-slim

WORKDIR /app
COPY . .

RUN rm -r venv
RUN pip3 install -r requirements.txt