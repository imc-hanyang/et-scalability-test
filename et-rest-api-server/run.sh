docker run -d -p 8000:8000 \
  -e ALLOWED_HOSTS=127.0.0.1 \
  --name et-rest-api-server qobiljon/et-rest-api-server:1.0