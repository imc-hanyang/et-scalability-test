#docker run --name et-scylla-1 --hostname et-scylla-1 -d scylladb/scylla --reactor-backend=epoll
#docker run --name et-scylla-2 --hostname et-scylla-2 -d scylladb/scylla --reactor-backend=epoll --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' et-scylla-1)"
#docker run --name et-scylla-3 --hostname et-scylla-3 -d scylladb/scylla --reactor-backend=epoll --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' et-scylla-1)"
#
## docker network create et-network
#
#docker network connect et-network et-scylla-1
#docker network connect et-network et-scylla-2
#docker network connect et-network et-scylla-3

docker run -d -p 9042:9042 \
  --name et-scylla-db \
  scylladb/scylla