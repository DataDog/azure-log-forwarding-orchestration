FROM ubuntu:24.04
RUN apt-get update
RUN apt-get install -y binutils zip python3 python3-dev python3-pip python3-venv
RUN python3 -m venv /venv
RUN source /venv/bin/activate
COPY ./pyproject.toml /deps/pyproject.toml
RUN pip install './deps[dev]'
