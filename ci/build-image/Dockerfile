FROM registry.ddbuild.io/images/mirror/ubuntu:24.04
RUN apt-get update
RUN apt-get install -y python3.12 python3.12-dev python3-pip python3.12-venv
RUN apt-get install -y golang-go
RUN apt-get install -y binutils zip jq

# Install deps for control plane
RUN python3.12 -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY ./control_plane /control_plane_deps
RUN pip install './control_plane_deps[dev]'

# Install deps for forwarder
RUN go install github.com/boumenot/gocover-cobertura@latest
ENV PATH="/root/go/bin:$PATH"
