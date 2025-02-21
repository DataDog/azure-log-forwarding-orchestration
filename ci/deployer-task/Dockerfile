FROM python:3.11-alpine


# Download dependencies
COPY ./pyproject.toml ./pyproject.toml
RUN mkdir ./tasks ./cache
RUN touch ./tasks/__init__.py ./cache/__init__.py
RUN python3.11 -m pip install -e '.[deployer_task]'

# Copy the code into the container
COPY ./cache ./cache
COPY ./tasks ./tasks

CMD ["python3.11", "-m", "tasks.deployer_task"]
