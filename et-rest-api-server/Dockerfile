FROM python:3.9.19-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install -U gunicorn
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8000

ENTRYPOINT ["python", "-m", "gunicorn"]
CMD ["et-rest-api-server.asgi:application", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000"]