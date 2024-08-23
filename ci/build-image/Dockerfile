FROM registry.ddbuild.io/images/mirror/ubuntu:24.04
RUN apt-get update
RUN apt-get install -y software-properties-common golang-go binutils zip jq ca-certificates curl
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update
RUN apt-get install -y python3.11 python3-pip python3.11-venv

# Install docker
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
RUN systemctl start docker
RUN docker run hello-world

# Install deps for control plane
RUN python3.11 -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY ./control_plane /control_plane_deps
RUN pip install './control_plane_deps[dev]'

# Install deps for forwarder
RUN go install github.com/boumenot/gocover-cobertura@latest
ENV PATH="/root/go/bin:$PATH"
