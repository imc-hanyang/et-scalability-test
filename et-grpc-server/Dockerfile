# base image
FROM python:3.9-slim

# environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies (gcc-c++ python3-devel)
RUN apt-get update && apt-get install -y gcc g++ python3-dev

# set working directory
RUN mkdir /home/et_grpc
COPY . /home/et_grpc
WORKDIR /home/et_grpc

# install dependencies
RUN pip install -r requirements.txt

# open ports
EXPOSE 5432

# run web server
CMD ["python", "server.py"]

# build: `docker build -t et-grpc-server:1.0 .`
# create network: `docker network create et-network`
# run: `docker run -p 5432:5432 --network et-network --name et-grpc-server -d et-grpc-server:1.0`