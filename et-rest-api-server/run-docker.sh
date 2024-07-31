docker run -d -p 8000:8000 \
  -e CASSANDRA_IP_ADDRESSES=172.17.0.2,172.17.0.3,172.17.0.4 \
  -e CASSANDRA_ADMIN_USER=cassandra \
  -e CASSANDRA_ADMIN_PASSWORD=cassandra \
  --name et-rest-api-server qobiljon/et-rest-api-server:1.0

docker network connect et-network et-rest-api-server

#docker run -d -p 8000:8000 \
#	--network et-network \
#	--add-host=host.docker.internal:host-gateway \
#	-e CASSANDRA_IP_ADDRESSES=host.docker.internal \
#	-e CASSANDRA_ADMIN_USER=cassandra \
#	-e CASSANDRA_ADMIN_PASSWORD=cassandra \
#	--name et-rest-api-server qobiljon/et-rest-api-server:1.0