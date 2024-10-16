start_time=$(date +%s)
curl -X POST http://127.0.0.1:8000/make_cpu_busy/ \
  -H "Content-Type: multipart/form-data" \
  -F "duration_sec=10"
end_time=$(date +%s)
echo ""
echo "Duration: $((end_time - start_time)) seconds"
