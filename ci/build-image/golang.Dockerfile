FROM registry.ddbuild.io/docker:24.0.4-gbi-focal

RUN apt-get update
RUN apt-get install -y golang-go
