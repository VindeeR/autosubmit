#!/bin/bash

# Run jupyter lab as daemon and assign token if env variable exists
if [ -n "$JUPYTER_TOKEN" ]; then
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab --NotebookApp.token=$JUPYTER_TOKEN --allow-root &
else
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab --allow-root &
fi

# Run the command passed by docker run
autosubmit_api start --log-level=DEBUG -b 0.0.0.0:8000
