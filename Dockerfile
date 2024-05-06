FROM ubuntu:20.04

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt update \
    && apt upgrade -y \
    && apt install -y python3 \
    && apt install -y python3-pip \
    && apt install -y curl \
    && apt install -y vim \
    && apt install -y jq

ARG FOLDERNAME=redfish_exporter
ARG CONFIG=config

RUN mkdir /${FOLDERNAME}
RUN mkdir /${CONFIG}

RUN mkdir /${FOLDERNAME}/collectors

WORKDIR /${FOLDERNAME}

RUN pip3 install --upgrade pip
COPY requirements.txt /${FOLDERNAME}
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py /${FOLDERNAME}/
COPY collectors/ /${FOLDERNAME}/collectors/
COPY config.yml /${CONFIG}/

