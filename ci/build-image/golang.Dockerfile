FROM registry.ddbuild.io/docker:24.0.4-gbi-focal

RUN apt-get update
RUN apt-get install -y curl

RUN curl -OL https://golang.org/dl/go1.22.2.linux-amd64.tar.gz
RUN tar -C /usr/local -xvf go1.22.2.linux-amd64.tar.gz
RUN rm go1.22.2.linux-amd64.tar.gz

ENV PATH=$PATH:/usr/local/go/bin
