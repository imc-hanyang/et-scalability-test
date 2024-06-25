# base image
FROM python:3.9-slim

# environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# set working directory
RUN mkdir /home/et_dashboard
COPY . /home/et_dashboard
WORKDIR /home/et_dashboard

# install dependencies
RUN pip install -r requirements.txt
#RUN pip install gunicorn
#RUN pip install pyOpenSSL

# migrate database
RUN python manage.py makemigrations
RUN python manage.py migrate

# run django server
EXPOSE 8080
CMD ["python", "manage.py", "runserver", "0:8080"]

# build: `docker build -t et-dashboard:1.0 .`
# create network: `docker network create et-network`
# run: `docker run -p 8080:8080 --network et-network --name et-dashboard -d et-dashboard:1.0`