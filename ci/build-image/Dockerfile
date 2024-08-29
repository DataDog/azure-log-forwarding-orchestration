FROM registry.ddbuild.io/images/mirror/ubuntu:24.04
RUN apt-get update
RUN apt-get install -y software-properties-common binutils zip jq ca-certificates curl
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update
RUN apt-get install -y python3.11 python3-pip python3.11-venv

# Install go 1.22.2
RUN curl -OL https://golang.org/dl/go1.22.2.linux-amd64.tar.gz
RUN tar -C /usr/local -xvf go1.22.2.linux-amd64.tar.gz
RUN rm go1.22.2.linux-amd64.tar.gz

ENV PATH=$PATH:/usr/local/go/bin

# Install deps for control plane
RUN python3.11 -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY ./control_plane /control_plane_deps
RUN pip install './control_plane_deps[dev]'

# Install deps for forwarder
RUN go install github.com/boumenot/gocover-cobertura@latest
ENV PATH="/root/go/bin:$PATH"
