# Carfax S3 Ingestion Worker Lambda

`grsdiai-hydration-lambda-carfax-s3-ingestion-worker` processes inbound Venti/Carfax SQS events, applies a waterfall resolution strategy to extract unique business identifiers (VIN or header ID), and writes clean payloads to S3. Any contract-violating, malformed, or unidentifiable messages are isolated and archived to an S3 error quarantine prefix with full execution traces.

---

## 1. Overview & Workflow

```text
               +-----------------------------+
               | Inbound SQS Queue / FIFO    |
               +--------------+--------------+
                              |
                              v
             +----------------------------------+
             | lambda_handler (index.py)        |
             | - LambdaConfig & LambdaUtility   |
             | - JSON Structured Logging        |
             +----------------+-----------------+
                              |
                     parse_payload(body)
                              |
                              v
             +----------------------------------+
             | extract_business_identifier()    |
             | 1. event.header.id               |
             | 2. payload.vin / data.vin        |
             | 3. recursive vin scan            |
             | 4. payload.id                    |
             +----------------+-----------------+
                              |
               +--------------+--------------+
               |                             |
      (Success: Valid ID)           (Failure: Exception / Invalid)
               |                             |
               v                             v
  +--------------------------+  +--------------------------+
  | s3.put_object            |  | handle_record_failure    |
  | Target: s3://<BUCKET>/   |  | s3.put_object            |
  | <S3_PREFIX>/<PARTITION>/ |  | Target: s3://<BUCKET>/   |
  | <clean_id>.json          |  | <ERROR_PREFIX>/          |
  +--------------------------+  | <PARTITION>/             |
                                | failed_<messageId>.json  |
                                +--------------------------+
```

1. **Trigger**: An SQS queue feeds batch events to the function.
2. **Context & Logging Initialization**: Initializes `LambdaConfig` and `LambdaUtility` to stream structured JSON logs to CloudWatch.
3. **Payload Extraction**: Unwraps JSON payloads whether wrapped inside EventBridge `detail` envelopes or delivered directly as raw SQS strings.
4. **Identifier Resolution**: Determines the target S3 filename using a waterfall resolution strategy and sanitizes illegal S3 path characters.
5. **Partitioning**: Generates an hourly partition key formatted as `YYYY-MM-DD-HH` using UTC timestamps.
6. **Persistence**:
   - **Valid records**: Serialized using compact separators (`separators=(',', ':')`) and saved under `s3://<LANDING_BUCKET>/<S3_PREFIX>/<YYYY-MM-DD-HH>/<file_id>.json`.
   - **Failed records**: Quarantined under `s3://<LANDING_BUCKET>/<ERROR_PREFIX>/<YYYY-MM-DD-HH>/failed_<messageId>.json` containing stack trace diagnostics, ISO timestamp, and the raw source message.

---

## 2. Environment Variables

All AWS SDK client configurations and S3 destination paths are resolved dynamically at runtime using environment variables:

| Variable Name | Required | Description | Example / Target Value |
| :--- | :---: | :--- | :--- |
| `LANDING_BUCKET` | **Yes** | Destination S3 bucket for both processed and error records | `grs-zone1-us-landing-development` |
| `S3_PREFIX` | **Yes** | S3 key prefix where successfully processed payloads are placed | `inbound/venti_json_ingestion/third_party/carfax` |
| `ERROR_PREFIX` | **Yes** | S3 key prefix where quarantined error files are placed | `inbound/venti_json_ingestion/third_party/carfax_error` |
| `TARGET_REGION` | No | Overrides client target region (falls back to `AWS_REGION` or `AWS_Region`) | `us-east-1` |
| `AWS_REGION` | Auto | Natively injected by the AWS Lambda runtime environment | `us-east-1` |

---

## 3. Framework & Architecture Standards

### Shared Providers Integration
This function follows team architectural standards by delegating configuration parsing and telemetry to the shared `providers` module:

```python
from providers.lambda_config import LambdaConfig
from providers.lambda_utils import LambdaUtility
```

- **Zero Direct Logging Setup**: Eliminates raw `import logging` calls and handler definitions inside `index.py`. 
- **`LambdaConfig(event, context)`**: Automatically extracts incoming event parameters, memory limits, request IDs, and function ARNs.
- **`LambdaUtility(config)`**: Exposes `utility.logger`, which formats CloudWatch output according to the team's `Logger.JsonFormatter` schema.

### Cyclomatic Complexity & Quality Gates
The codebase conforms to SonarQube / Flake8 rules:
- **`lambda_handler`**: Maintained under 20 lines of code (well beneath the 50-line limit) by offloading payload extraction, business identifier extraction, and quarantine routing to dedicated modular helpers.
- **Function Complexity Limits**: `find_key_recursive`, `get_header_id`, `get_nested_vin`, and `extract_business_identifier` maintain a cyclomatic complexity $\le 6$ (limit is 8).

---

## 4. Failure Modes & Quarantine Semantics

| Scenario | Destination Path | SQS Queue Impact | Investigation Action |
| :--- | :--- | :--- | :--- |
| **Valid Record** | `s3://<LANDING_BUCKET>/<S3_PREFIX>/<partition>/<id>.json` | Message deleted from queue | Normal pipeline downstream ingestion. |
| **Missing VIN / Malformed JSON** | `s3://<LANDING_BUCKET>/<ERROR_PREFIX>/<partition>/failed_<messageId>.json` | Message acknowledged & deleted (prevents poison-pill loop) | Inspect quarantine payload in S3 to examine the `error_message`, `error_trace`, and `original_record`. |
| **AWS Infrastructure Failure (S3 Outage / IAM Permission Error)** | CloudWatch Logs (`Runtime error`) | Unacknowledged; retried until moving to SQS DLQ | Inspect CloudWatch log stream using `@message like /AccessDenied/` or check IAM execution role policies. |

---

## 5. Directory Layout

### Repository Structure
```text
repo_root/
├── src/lambda/
│   ├── utilities/
│   │   └── providers/
│   │       ├── __init__.py
│   │       ├── lambda_config.py
│   │       ├── lambda_utils.py
│   │       └── logger.py
│   └── grsdiai-hydration-lambda-carfax-s3-ingestion-worker/
│       ├── index.py
│       └── README.md
```

### AWS Lambda Runtime Structure (Console / Deployment Artifact)
When packaged by CI/CD (or deployed manually in the console), `providers` sits adjacent to `index.py`:
```text
/var/task/
├── index.py
└── providers/
    ├── __init__.py
    ├── lambda_config.py
    ├── lambda_utils.py
    └── logger.py
```

---

## 6. CloudWatch Logs Insights Queries

### Query Processed Payloads
```sql
fields @timestamp, message, metadata.aws_request_id, metadata.execution_id
| filter message like /Writing valid payload/
| sort @timestamp desc
| limit 50
```

### Query Quarantine & Errors
```sql
fields @timestamp, message, metadata.aws_request_id, exc_text
| filter level = 'ERROR' or message like /Processing failed for message/
| sort @timestamp desc
| limit 50
```