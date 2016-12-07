FROM ubuntu
MAINTAINER Yuxing Wang <wangyx2005@gmail.com>

RUN apt-get update && apt-get install -y \
    python3

RUN mkdir -p /tmp/work

WORKDIR /tmp/work

COPY compute.py compute.py