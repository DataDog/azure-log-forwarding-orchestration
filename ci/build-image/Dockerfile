FROM ghcr.io/astral-sh/uv:python3.11-bookworm
RUN apt-get update
RUN apt-get install -y software-properties-common binutils zip jq ca-certificates curl

# Install go 1.22.2
RUN curl -OL https://golang.org/dl/go1.22.2.linux-amd64.tar.gz
RUN tar -C /usr/local -xvf go1.22.2.linux-amd64.tar.gz
RUN rm go1.22.2.linux-amd64.tar.gz

ENV PATH=$PATH:/usr/local/go/bin

# Install deps for control plane
RUN uv venv /venv
ENV PATH="/venv/bin:$PATH"
COPY ./control_plane /control_plane_deps
RUN uv pip install './control_plane_deps[dev]'

# Install deps for forwarder
RUN go install github.com/boumenot/gocover-cobertura@latest
ENV PATH="/root/go/bin:$PATH"
