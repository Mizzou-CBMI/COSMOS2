FROM ubuntu:14.04
MAINTAINER Erik Gafni

RUN apt-get update -y
RUN apt-get install -y git python-dev python-pip
RUN pip install pip -U
RUN pip install setuptools -U
#RUN pip install virtualenv

RUN git clone https://github.com/LPM-HMS/COSMOS2.git Cosmos
WORKDIR Cosmos

#RUN virtualenv ve
#RUN source ve/bin/activate
RUN pip install .
RUN python setup.py develop

EXPOSE 5000

