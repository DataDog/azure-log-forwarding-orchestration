FROM python:3.11-alpine

# Copy the code into the container
COPY ./cache ./cache
COPY ./tasks ./tasks
COPY ./pyproject.toml ./pyproject.toml

# Download dependencies
RUN python3.11 -m pip install '.[deployer_task]'

CMD ["python3.11", "-m", "tasks.deployer_task"]
