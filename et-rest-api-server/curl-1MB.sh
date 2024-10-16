start_time=$(date +%s)
curl -X POST http://127.0.0.1:8000/upload/ \
  -H "Content-Type: multipart/form-data" \
  -F "user_id=1" \
  -F "timestamp=$(python -c 'import time; print(int(time.time() * 1000))')" \
  -F "file=@/Users/qobiljon/Desktop/et-scalability-test/jmeter/data/1MB.txt"
end_time=$(date +%s)
echo ""
echo "1MB case elapsed time: $((end_time-start_time)) seconds"
