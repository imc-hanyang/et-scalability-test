# base image
FROM python:3.9-slim

# environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# set working directory
RUN mkdir /home/et_dashboard
COPY . /home/et_dashboard
WORKDIR /home/et_dashboard

# install dependencies
RUN pip install -r requirements.txt
RUN #pip install gunicorn
# RUN pip install pyOpenSSL
RUN python manage.py makemigrations
RUN python manage.py migrate

# open ports
EXPOSE 8080
# EXPOSE 8443

# run web server
 CMD ["python", "manage.py", "runserver", "0:8080"]
#CMD ["gunicorn", "-c", "gunicorn_config.py", "ET_Dashboard.wsgi"]
