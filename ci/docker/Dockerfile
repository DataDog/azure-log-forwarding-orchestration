FROM ubuntu:latest
RUN apt update && apt install -y software-properties-common wget
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y python3.11
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.11 get-pip.py
RUN mkdir /opt/app
WORKDIR /opt/app
COPY build_requirements.txt .
RUN pip install -r build_requirements.txt
RUN mkdir diagnostic_settings_task resources_task
COPY diagnostic_settings_task/requirements.txt ./diagnostic_settings_task/requirements.txt
RUN pip install -r ./diagnostic_settings_task/requirements.txt
COPY resources_task/requirements.txt ./resources_task/requirements.txt
RUN pip install -r ./resources_task/requirements.txt
COPY diagnostic_settings_task ./diagnostic_settings_task
COPY resources_task ./resources_task
CMD [ "pytest", "./diagnostic_settings_task/tests" ]
