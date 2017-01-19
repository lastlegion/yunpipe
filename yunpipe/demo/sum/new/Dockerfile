FROM sum
MAINTAINER Yuxing Wang <wangyx2005@gmail.com>

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    zip

RUN python3 -m pip install boto3

RUN mkdir -p /tmp/work

WORKDIR /tmp/work

COPY runscript.py runscript.py

CMD python3 runscript.py