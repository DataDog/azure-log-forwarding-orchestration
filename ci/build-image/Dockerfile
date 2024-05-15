FROM registry.ddbuild.io/images/python:3.11.6
USER root
RUN apt-get update && apt-get install binutils -y
USER dog
RUN pip install -U pyinstaller
ENV PATH="${HOME}/.local/bin:${PATH}"
COPY ./config/ /config/
RUN find /config/ -name 'requirements.txt' -exec pip install -r {} \;
