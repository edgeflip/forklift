FROM ubuntu:14.04
RUN apt-get update && apt-get install -y wget git python-dev libmysqlclient-dev build-essential virtualenvwrapper libpq-dev python-setuptools python-numpy python-scipy libatlas-dev libatlas-base-dev liblapack-dev gfortran 
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python ./get-pip.py
ADD . /app
WORKDIR /app
RUN pip install -r ./requirements/base.requirements
