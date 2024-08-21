FROM python:3.9-slim-bookworm

RUN apt-get update && apt-get install -y graphviz git wget curl gcc libpq-dev;

RUN pip3 install \
        git+https://earth.bsc.es/gitlab/es/autosubmit.git@postgres-support#egg=autosubmit[all] \
        git+https://earth.bsc.es/gitlab/es/autosubmit-api.git@postgres-support#egg=autosubmit_api[all] \
        jupyterlab;


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app

COPY ./autosubmitrc /app/autosubmitrc

ENV AUTOSUBMIT_CONFIGURATION=/app/autosubmitrc

ENV PROTECTION_LEVEL=NONE
ENV JUPYTER_TOKEN="autosubmit"

EXPOSE 8888
EXPOSE 8000

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
