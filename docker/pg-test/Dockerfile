FROM python:3.9-slim-bookworm

RUN apt-get update && apt-get install -y graphviz git wget curl gcc libpq-dev;

RUN pip3 install git+https://earth.bsc.es/gitlab/es/autosubmit.git@postgres-support#egg=autosubmit[all];

WORKDIR /app

COPY ./autosubmitrc /app/autosubmitrc

ENV AUTOSUBMIT_CONFIGURATION=/app/autosubmitrc

CMD tail -f /dev/null
