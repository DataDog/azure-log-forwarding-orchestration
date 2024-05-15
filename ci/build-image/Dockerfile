FROM registry.ddbuild.io/images/python:3.11.6
USER root
RUN apt-get update && apt-get install binutils python3-dev -y
USER dog
RUN pip install -U pyinstaller
COPY ./config/ /config/
RUN find /config/ -name 'requirements.txt' -exec pip install -r {} \;
