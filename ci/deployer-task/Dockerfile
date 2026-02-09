FROM registry.ddbuild.io/images/mirror/python:3.11.5-alpine3.18

RUN mkdir /app
WORKDIR /app
# Download dependencies
RUN pip install --upgrade pip wheel
RUN echo "setuptools<70" > /tmp/constraints.txt
COPY ./pyproject.toml ./pyproject.toml
RUN PIP_CONSTRAINT=/tmp/constraints.txt python3.11 -m pip install -e '.[deployer_task]'

# Copy the code into the container
COPY ./cache ./cache
COPY ./tasks ./tasks

# Set the version
ARG VERSION_TAG="latest"
ENV VERSION_TAG=$VERSION_TAG

CMD ["python3.11", "-m", "tasks.deployer_task"]
