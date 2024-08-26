FROM 486234852809.dkr.ecr.us-east-1.amazonaws.com/docker:24.0.4-gbi-focal

RUN apt-get update
RUN apt-get install -y golang-go
