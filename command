aws s3 ls s3://grs-zone1-us-landing-development/inbound/venti_json_ingestion/carfax/2026-10-07-08/ --recursive


curl -X POST "YOUR_INVOKE_URL/venti-event" \
  -H "Content-Type: application/json" \
  -H "x-partition-date: 2026-10-15-05" \
  -H "x-partition-shard: 3f" \
  -d '{"test_id": "http_stream_trigger", "status": "ok"}'
