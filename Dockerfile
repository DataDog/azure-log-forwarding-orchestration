FROM ubuntu:latest
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
