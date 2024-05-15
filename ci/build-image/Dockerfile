FROM registry.ddbuild.io/images/python:3.11.6
RUN sudo apt-get install binutils
RUN pip install -U pyinstaller
ENV PATH="${HOME}/.local/bin:${PATH}"
COPY config/ ./
RUN pip install -r config/requirements.txt -r config/*/requirements.txt
