docker run -d -p 8000:8000 \
	--network et-network \
	-e CASSANDRA_IP_ADDRESSES=et-cassandra-1,et-cassandra-2,et-cassandra-3 \
	-e CASSANDRA_ADMIN_USER=cassandra \
	-e CASSANDRA_ADMIN_PASSWORD=cassandra \
	--name et-rest-api-server qobiljon/et-rest-api-server:1.0


#docker run -d -p 8000:8000 \
#	--network et-network \
#	--add-host=host.docker.internal:host-gateway \
#	-e CASSANDRA_IP_ADDRESSES=host.docker.internal \
#	-e CASSANDRA_ADMIN_USER=cassandra \
#	-e CASSANDRA_ADMIN_PASSWORD=cassandra \
#	--name et-rest-api-server qobiljon/et-rest-api-server:1.0