# Component Specification: Amazon S3 Landing & Quarantine Lake

## 1. Overview & Purpose
Amazon S3 acts as the ultimate durable storage layer for all incoming Carfax events. To adhere to the **Zero Data Loss** principle, the storage layout is partitioned into two dedicated destination directories:
1. **Primary Landing (`carfax/`):** Houses business-compliant JSON records organized by timestamp partitions for downstream consumption by data lakes and analytics engines.
2. **Quarantine Error Lake (`carfax_error/`):** Stores complete audit payloads for structurally invalid or contract-violating records, encapsulating the raw input, root-cause error, and stack trace.

---

## 2. Bucket & Partitioning Hierarchy

* **Bucket Name:** `grs-zone1-us-landing-development`
* **Storage Class:** S3 Standard
* **Base Root Key:** `inbound/venti_json_ingestion/third_party/`

```text
s3://grs-zone1-us-landing-development/inbound/venti_json_ingestion/third_party/
│
├── carfax/                             <── Primary Clean Folder
│   └── YYYY-MM-DD-HH/                  <── Hourly UTC Partition
│       ├── CARFAX_EVT_1001.json
│       └── 1HGCR2F8XHA000000.json
│
└── carfax_error/                       <── Quarantine Error Folder
    └── YYYY-MM-DD-HH/                  <── Hourly UTC Partition
        ├── failed_msg_1729675815.json
        └── failed_msg_1729675880.json
```

---

## 3. Object Schemas

### Clean Valid Record (`carfax/<YYYY-MM-DD-HH>/<id>.json`)
Contains the clean, parsed Carfax business payload ready for downstream ETL:
```json
{
  "event": {
    "header": {
      "id": "CARFAX_EVT_1001"
    }
  },
  "vin": "1HGCR2F8XHA000000",
  "data": {
    "mileage": 42150,
    "odometer_unit": "miles",
    "service_records": [
      {
        "date": "2026-05-12",
        "description": "Oil and filter changed"
      }
    ]
  }
}
```

### Quarantined Record (`carfax_error/<YYYY-MM-DD-HH>/failed_<msgId>.json`)
Contains the diagnostic wrapper enclosing the original SQS record and failure metadata:
```json
{
  "error_message": "Missing required business identifier: 'event.header.id' or 'vin'",
  "error_trace": "Traceback (most recent call last):\n  File \"/var/task/Code/index.py\", line 49, in lambda_handler\n    raise ValueError(\"Missing required business identifier: 'event.header.id' or 'vin'\")\nValueError: Missing required business identifier: 'event.header.id' or 'vin'",
  "timestamp_utc": "2026-10-23T09:30:15.123456+00:00",
  "original_record": {
    "messageId": "msg_1729675815",
    "receiptHandle": "AQEB...",
    "body": "{\"version\":\"0\",\"id\":\"e4a2c1b8-...\",\"source\":\"venti.api.gateway\",\"detail\":{\"event\":{\"header\":{\"id\":\"\"}},\"payload\":{\"status\":\"missing_id\"}}}",
    "attributes": {
      "ApproximateReceiveCount": "1",
      "SentTimestamp": "1729675815000",
      "SequenceNumber": "1888...",
      "MessageGroupId": "default"
    }
  }
}
```

---

## 4. Operational Invariant: Bucket Object Count as Truth

Because CloudWatch SQS metrics are aggregated samples over 1-minute or 5-minute periods, **CloudWatch graphs may show lower counts (e.g., 300 receive operations) during batch consumption of 900+ files**. 

The authoritative confirmation of pipeline throughput is verified via S3 object counts:

```bash
# Verify valid records for a specific hour
aws s3 ls s3://grs-zone1-us-landing-development/inbound/venti_json_ingestion/third_party/carfax/2026-10-23-09/ --summarize

# Verify quarantined error records for a specific hour
aws s3 ls s3://grs-zone1-us-landing-development/inbound/venti_json_ingestion/third_party/carfax_error/2026-10-23-09/ --summarize
```

---

## 5. Quarantine Reprocessing Procedure (Replay)

When an upstream contractor corrects a contract discrepancy, quarantined records can be reprocessed without requesting data re-transmission:

1. Identify the target error partition: `s3://.../carfax_error/YYYY-MM-DD-HH/`.
2. Execute an administrative replay script that extracts `original_record.body` from each failed file.
3. Apply any required schema transformation.
4. Repost to the API Gateway `/venti-event` endpoint or write directly to the primary `carfax/` partition.
5. Archive or purge the reprocessed `failed_*.json` objects.