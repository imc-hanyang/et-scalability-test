docker run \
  -d \
  -p 9042:9042 \
  --volume ./scylla.yaml:/etc/scylla/scylla.yaml \
  --name scylla-node1 \
  --hostname scylla-node1 \
  scylladb/scylla:6.0.2 --reactor-backend=epoll
docker run \
  -d \
  --volume ./scylla.yaml:/etc/scylla/scylla.yaml \
  --name scylla-node2 \
  --hostname scylla-node2 \
  scylladb/scylla:6.0.2 --reactor-backend=epoll --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-node1)"
docker run \
  -d \
  --volume ./scylla.yaml:/etc/scylla/scylla.yaml \
  --name scylla-node3 \
  --hostname scylla-node3 \
  scylladb/scylla:6.0.2 --reactor-backend=epoll --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-node1)"

docker exec -it scylla-node1 nodetool disableautocompaction
docker exec -it scylla-node2 nodetool disableautocompaction
docker exec -it scylla-node3 nodetool disableautocompaction