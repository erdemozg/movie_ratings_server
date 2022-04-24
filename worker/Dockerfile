FROM python:3.8-slim-buster

WORKDIR /

RUN pip install --upgrade pip

RUN apt-get update \
    && apt-get install git -y \
    && apt-get clean

COPY requirements.txt /worker/requirements.txt

RUN pip install -r /worker/requirements.txt

CMD [ "python", "-u", "/worker/main.py" ]
