#======================================================================================================================#
#                                            BUILDER CONTAINER                                                         #
#======================================================================================================================#

FROM python:3.6.5 AS builder

#build args
ARG VENV_DIR=/venv
ARG PIP_EXEC=$VENV_DIR/bin/pip
ARG PYTHON_EXEC=$VENV_DIR/bin/python
ARG TEMP_DIR=/tmp
ARG UTILS_DIR=/utils

#copy requirements
COPY requirements.txt .

#install requirements in virtualenv
RUN pip install virtualenv && \
    virtualenv $VENV_DIR && \
    $PIP_EXEC install -r requirements.txt

#install utils
RUN apt-get update && apt-get install -y git
RUN git clone https://github.com/vishnubob/wait-for-it.git $TEMP_DIR/wait-for-it \
        && mkdir $UTILS_DIR \
        && cp  $TEMP_DIR/wait-for-it/wait-for-it.sh $UTILS_DIR/wait-for-it.sh \
        && rm -rf TEMP_DIR

#======================================================================================================================#
#                                            MAIN    CONTAINER                                                         #
#======================================================================================================================#

FROM python:3.6.5-slim

#build args
ARG SRC=/src
ARG VENV_DIR=/venv
ARG UTILS_DIR=/utils

#copy environment and compiled proto files from builder container and source files
COPY --from=builder $VENV_DIR $VENV_DIR
COPY --from=builder $UTILS_DIR $UTILS_DIR
COPY . $SRC

WORKDIR $SRC

ENTRYPOINT ["/venv/bin/python", "run.py"]
