# base image
# FROM python:3.9-slim
# environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# set working directory
RUN mkdir /home/et_grpc
COPY . /home/et_grpc
WORKDIR /home/et_grpc

# install dependencies
RUN apt-get update && apt-get install -y gcc g++ python3-dev
RUN pip install -r requirements.txt

# run grpc server
EXPOSE 50051
ENTRYPOINT ["python", "server.py"]

# build: `docker build -t qobiljon/et-grpc-server:1.0 .`
# create network: `docker network create et-network`
# run: `docker run -p 5432:5432 --network et-network --name et-grpc-server -d qobiljon/et-grpc-server:1.0`
