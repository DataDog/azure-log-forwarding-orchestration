FROM registry.ddbuild.io/images/python:3.11.6
USER root
RUN apt-get install binutils
USER dog
RUN pip install -U pyinstaller
ENV PATH="${HOME}/.local/bin:${PATH}"
COPY config/ ./
RUN pip install -r config/requirements.txt -r config/*/requirements.txt
