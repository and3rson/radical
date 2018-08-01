FROM alpine:3.7

ENV PYTHONUNBUFFERED 1

RUN apk add --no-cache busybox bash make python3 python3-dev py3-pip gcc musl-dev postgresql-dev && \
    rm -rf /var/cache/apk/*

WORKDIR /usr/src/app

COPY pytest.ini /usr/src/app
COPY .coveragerc /usr/src/app
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt
COPY radical /usr/src/app/radical
