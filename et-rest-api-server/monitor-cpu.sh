while true; do
  echo "$(date):" >> cpu-stats-log.txt
  docker stats --no-stream >> cpu-stats-log.txt
  sleep 0.5
done

