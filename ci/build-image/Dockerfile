FROM ubuntu:24.04
RUN apt-get update
RUN apt-get install -y binutils zip python3 python3-dev python3-pip python3-venv
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"
RUN pip install pyinstaller
COPY ./config/ /config/
RUN find /config/ -name 'requirements.txt' -exec pip install -r {} \;
COPY ./requirements-dev.txt /requirements-dev.txt
RUN pip install -r /requirements-dev.txt
