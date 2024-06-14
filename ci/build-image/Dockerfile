FROM ubuntu:24.04
RUN apt-get update
RUN apt-get install -y python3.12 python3.12-dev python3-pip python3.12-venv
RUN apt-get install -y binutils zip jq
RUN python3.12 -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY ./pyproject.toml /deps/pyproject.toml
RUN pip install -e './deps[dev]'
