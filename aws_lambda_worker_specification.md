# Component Specification: AWS Lambda Ingestion Worker

## 1. Overview & Purpose
The `grsdiai-hydration-lambda-carfax-s3-ingestion-worker` function executes the core parsing, contract validation, and durable storage logic for the Carfax pipeline. It consumes batches from the SQS FIFO queue, verifies the payload structure against business invariants, writes clean records to `carfax/`, and diverts malformed records to `carfax_error/`.

---

## 2. Function Configuration Details

| Parameter | Value |
| :--- | :--- |
| **Function Name** | `grsdiai-hydration-lambda-carfax-s3-ingestion-worker` |
| **Runtime** | Python 3.11 / Python 3.12 |
| **Handler** | `Code/index.lambda_handler` |
| **Memory Allocation** | 512 MB |
| **Timeout** | 60 seconds |
| **Event Source** | Amazon SQS (`...-buffer.fifo`) |
| **Batch Size** | 10 records |
| **Batch Window** | 0 seconds (Immediate dispatch for FIFO) |
| **Target S3 Bucket** | `grs-zone1-us-landing-development` |

---

## 3. Worker Execution Logic & Code Flow

```text
Incoming SQS Record
       │
       ▼
   try block
       │
       ├─► 1. json.loads(record['body']) ──(Fail)──► [ValueError / JSONDecodeError] ──┐
       │                                                                             │
       ├─► 2. Extract EventBridge 'detail' ──────────────────────────────────────────┤
       │                                                                             │
       ├─► 3. Validate 'event.header.id'                                             │
       │      or root 'vin' ───────────────(Fail)──► [ValueError: Missing ID] ───────┤
       │                                                                             │
       ▼ (All Checks Pass)                                                           ▼
 [PASSED: Primary S3]                                                  [FAILED: Quarantine S3]
 Write clean payload to:                                               except block catches error
 inbound/.../carfax/<YYYY-MM-DD-HH>/<id>.json                          Write audit wrapper to:
                                                                       inbound/.../carfax_error/<YYYY-MM-DD-HH>/failed_<msgId>.json
       │                                                                             │
       └──────────────────────────────────────┬──────────────────────────────────────┘
                                              │
                                              ▼
                                   Returns HTTP 200 to SQS
                                 (Message Deleted from Queue)
```

### Python Lambda Handler Implementation (`Code/index.py`)
```python
import json
import logging
import os
import traceback
from datetime import datetime, timezone
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "grs-zone1-us-landing-development")
BASE_PREFIX = "inbound/venti_json_ingestion/third_party"

def extract_identifier(payload: dict) -> str:
    """Extracts mandatory event.header.id or root vin."""
    header_id = payload.get("event", {}).get("header", {}).get("id")
    if header_id and str(header_id).strip():
        return str(header_id).strip()
    
    vin = payload.get("vin")
    if vin and str(vin).strip():
        return str(vin).strip()
    
    raise ValueError("Missing required business identifier: 'event.header.id' or 'vin'")

def lambda_handler(event, context):
    records = event.get("Records", [])
    logger.info(f"Processing {len(records)} message(s) from SQS.")
    
    now = datetime.now(timezone.utc)
    hour_partition = now.strftime("%Y-%m-%d-%H")
    
    for record in records:
        msg_id = record.get("messageId", f"unknown_{int(now.timestamp())}")
        raw_body = record.get("body", "")
        
        try:
            # 1. Parse outer SQS body
            sqs_body = json.loads(raw_body)
            
            # 2. Extract EventBridge detail wrapper if present
            if isinstance(sqs_body, dict) and "detail" in sqs_body:
                inner_detail = sqs_body["detail"]
                payload_obj = json.loads(inner_detail) if isinstance(inner_detail, str) else inner_detail
            else:
                payload_obj = sqs_body
                
            if not isinstance(payload_obj, dict):
                raise ValueError(f"Payload root must be a JSON object, received {type(payload_obj).__name__}")
            
            # 3. Contract validation
            file_id = extract_identifier(payload_obj)
            
            # 4. Save Valid Record
            s3_key = f"{BASE_PREFIX}/carfax/{hour_partition}/{file_id}.json"
            logger.info(f"Writing valid payload to s3://{LANDING_BUCKET}/{s3_key}")
            s3_client.put_object(
                Bucket=LANDING_BUCKET,
                Key=s3_key,
                Body=json.dumps(payload_obj, indent=2),
                ContentType="application/json"
            )

        except Exception as exc:
            # 5. Quarantine Failure Flow
            logger.warning(f"[!] Processing failed for message {msg_id}: {str(exc)}")
            error_key = f"{BASE_PREFIX}/carfax_error/{hour_partition}/failed_{msg_id}.json"
            
            quarantine_payload = {
                "error_message": str(exc),
                "error_trace": traceback.format_exc(),
                "timestamp_utc": now.isoformat(),
                "original_record": record
            }
            
            logger.info(f"Archiving failed payload to s3://{LANDING_BUCKET}/{error_key}")
            s3_client.put_object(
                Bucket=LANDING_BUCKET,
                Key=error_key,
                Body=json.dumps(quarantine_payload, indent=2),
                ContentType="application/json"
            )

    return {"statusCode": 200, "message": "Batch processed successfully"}
```

---

## 4. IAM Execution Role & Policies

### Trust Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Permissions Policy (`grsdiai-hydration-lambda-carfax-s3-ingestion-worker-policy`)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/grsdiai-hydration-lambda-carfax-s3-ingestion-worker:*"
    },
    {
      "Sid": "SQSConsumerPrivileges",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:us-east-1:123456789012:grsdiai-hydration-sqs-carfax-ingestion-buffer.fifo"
    },
    {
      "Sid": "S3LakePutPrivileges",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::grs-zone1-us-landing-development/inbound/venti_json_ingestion/third_party/*"
    }
  ]
}
```

---

## 5. Operational Insights & Log Queries

**Log Group Name:**
`/aws/lambda/grsdiai-hydration-lambda-carfax-s3-ingestion-worker`

### CloudWatch Insights Query: Success vs. Quarantine Audit
```sql
fields @timestamp, @logStream, @message
| filter @message like /Writing valid payload/ or @message like /Archiving failed payload/
| sort @timestamp desc
| limit 100
```