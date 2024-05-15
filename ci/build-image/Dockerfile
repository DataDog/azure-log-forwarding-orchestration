FROM ubuntu:24.04
RUN apt-get update
RUN apt-get install -y binutils zip python3 python3-dev python3-pip
COPY ./pyproject.toml /deps/pyproject.toml
RUN pip install --break-system-packages './deps[dev]'
