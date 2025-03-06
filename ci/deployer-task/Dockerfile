FROM python:3.11-alpine

RUN mkdir /app
WORKDIR /app
# Download dependencies
COPY ./pyproject.toml ./pyproject.toml
RUN python3.11 -m pip install -e '.[deployer_task]'

# Copy the code into the container
COPY ./cache ./cache
COPY ./tasks ./tasks

CMD ["python3.11", "-m", "tasks.deployer_task"]
