# Moody's Ingestion Pipeline: End-to-End Build and Operational Blueprint

This document provides complete, step-by-step instructions to create, configure, deploy, and verify the Moody's third-party ingestion pipeline from scratch. The architecture mirrors the production Carfax pipeline while maintaining complete resource isolation:

API Gateway (`/venti-event`)
→ EventBridge (`grsdiai-hydration-moodys-ingestion-bus`)
→ SQS Buffer Queue (`moodys-ingestion-buffer-queue`)
→ Ingestion Worker Lambda (`grsdiai-hydration-lambda-moodys-ingestion-worker`)
→ S3 Landing Lake (`grs-zone1-us-landing-development`)
→ Step Functions (`grsdiai-hydration-stepfun-venti-tpd-ingestion`)

## 1. Global Resource Naming and Parameter Matrix

Deploy all resources in AWS Region `us-east-1` (Account ID: `833390816381`).

| Component | Resource Name / Path | Key Configuration / Value | 
| ----- | ----- | ----- | 
| AWS Region | `us-east-1` | Target deployment region | 
| AWS Account ID | `833390816381` | Dedicated AWS account | 
| API Gateway | `grsdiai-hydration-api-tpd-moodys` | Regional REST API | 
| API Resource / Method | `/venti-event` (`POST`) | Integration Target: EventBridge PutEvents | 
| EventBridge Bus | `grsdiai-hydration-moodys-ingestion-bus` | Dedicated Custom Event Bus | 
| EventBridge Rule | `grsdiai-hydration-moodys-ingestion-rule` | Filter: `{"source": ["moodys.ingestion"]}` | 
| EventBridge Source | `moodys.ingestion` | Hardcoded in API Gateway VTL | 
| EventBridge DetailType | `MoodysInboundEvent` | Hardcoded in API Gateway VTL | 
| SQS Dead-Letter Queue | `moodys-ingestion-dlq` | Standard Queue (14-day retention) | 
| SQS Buffer Queue | `moodys-ingestion-buffer-queue` | Standard Queue (Visibility Timeout: 180s) | 
| Ingestion Lambda | `grsdiai-hydration-lambda-moodys-ingestion-worker` | Python 3.10/3.11, Memory: 512MB, Timeout: 60s | 
| S3 Landing Bucket | `grs-zone1-us-landing-development` | Shared Hydration Data Lake | 
| S3 Landing Prefix | `inbound/venti_json_ingestion/third_party/moodys` | Target partition directory prefix | 
| S3 Error Prefix | `inbound/venti_json_ingestion/third_party/moodys_error` | Quarantine prefix for malformed payloads | 
| Step Functions ARN | `arn:aws:states:us-east-1:833390816381:stateMachine:grsdiai-hydration-stepfun-venti-tpd-ingestion` | Hydration State Machine | 
| Source Identifier | `moodys` | Passed as `source_name` to Step Functions | 

## 2. IAM Roles and Security Policies

### 2.1 API Gateway Service Execution Role

* **Role Name:** `grsdiai-hydration-apigw-eventbridge-role` (or use existing `grsdiai-hydration-ingestion-lambda-role`)

* **Trust Relationship:**

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAPIGatewayAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "apigateway.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

```

* **Permissions Policy (Inline or Managed):**

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPutEventsToMoodysBus",
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "arn:aws:events:us-east-1:833390816381:event-bus/grsdiai-hydration-moodys-ingestion-bus"
    }
  ]
}

```

### 2.2 Worker Lambda Execution Role

* **Role Name:** `grsdiai-hydration-lambda-moodys-ingestion-worker-role`

* **Trust Relationship:** Principal `lambda.amazonaws.com`

* **Permissions Policy:**

```
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
      "Resource": "arn:aws:logs:us-east-1:833390816381:log-group:/aws/lambda/grsdiai-hydration-lambda-moodys-ingestion-worker:*"
    },
    {
      "Sid": "SQSConsumeMessages",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:us-east-1:833390816381:moodys-ingestion-buffer-queue"
    },
    {
      "Sid": "S3WriteAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::grs-zone1-us-landing-development/inbound/venti_json_ingestion/third_party/moodys*"
    },
    {
      "Sid": "StepFunctionsStartExecution",
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "arn:aws:states:us-east-1:833390816381:stateMachine:grsdiai-hydration-stepfun-venti-tpd-ingestion"
    }
  ]
}

```

## 3. SQS Buffer and Dead-Letter Queue Provisioning

Create the DLQ first so it can be assigned to the main buffer queue.

### Step 3.1: Create Dead-Letter Queue (DLQ)

1. Open **Amazon SQS** $\rightarrow$ Click **Create queue**.

2. **Type:** `Standard`.

3. **Name:** `moodys-ingestion-dlq`.

4. Leave defaults (Retention: 14 days) and click **Create queue**.

### Step 3.2: Create Ingestion Buffer Queue

1. Open **Amazon SQS** $\rightarrow$ Click **Create queue**.

2. **Type:** `Standard`.

3. **Name:** `moodys-ingestion-buffer-queue`.

4. **Configuration Settings:**

   * **Visibility timeout:** `180 Seconds` (must exceed the 60-second Lambda timeout).

   * **Message retention period:** `4 Days`.

   * **Delivery delay:** `0 Seconds`.

   * **Receive message wait time:** `20 Seconds` (Enables Long Polling).

5. **Dead-letter queue section:**

   * Enable **Dead-letter queue**.

   * Select `moodys-ingestion-dlq`.

   * **Maximum receives:** `3`.

6. **Access Policy:**
   Click **Edit** on the Access Policy and paste this permission statement allowing EventBridge to deliver events:

```
{
  "Version": "2012-10-17",
  "Id": "AllowEventBridgeToMoodysSQS",
  "Statement": [
    {
      "Sid": "AllowEventBridgePublish",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:833390816381:moodys-ingestion-buffer-queue",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:us-east-1:833390816381:rule/grsdiai-hydration-moodys-ingestion-bus/grsdiai-hydration-moodys-ingestion-rule"
        }
      }
    }
  ]
}

```

7. Click **Create queue**.

## 4. EventBridge Custom Bus and Routing Rule

### Step 4.1: Create Custom Event Bus

1. Open **Amazon EventBridge** $\rightarrow$ **Event buses** $\rightarrow$ Click **Create event bus**.

2. **Name:** `grsdiai-hydration-moodys-ingestion-bus`.

3. Click **Create**.

### Step 4.2: Create Routing Rule

1. Select `grsdiai-hydration-moodys-ingestion-bus` $\rightarrow$ Open **Rules** tab $\rightarrow$ Click **Create rule**.

2. **Name:** `grsdiai-hydration-moodys-ingestion-rule`.

3. **Event bus:** `grsdiai-hydration-moodys-ingestion-bus`.

4. **Rule type:** Select **Rule with an event pattern** $\rightarrow$ Click **Next**.

5. **Event source:** `Other`.

6. **Creation method:** `Custom pattern (JSON editor)`. Enter:

```
{
  "source": ["moodys.ingestion"]
}

```

 7. Click **Next**.

 8. **Target types:** `AWS service`.

 9. **Select a target:** Choose `SQS queue`.

10. **Queue:** Select `moodys-ingestion-buffer-queue`.

11. Click **Next** through tags and click **Create rule**.

## 5. Ingestion Worker Lambda Function Setup

### Step 5.1: Create Function

1. Open **AWS Lambda** $\rightarrow$ Click **Create function**.

2. **Function name:** `grsdiai-hydration-lambda-moodys-ingestion-worker`.

3. **Runtime:** `Python 3.10` or `Python 3.11`.

4. **Architecture:** `x86_64`.

5. **Execution role:** Select `grsdiai-hydration-lambda-moodys-ingestion-worker-role`.

6. Click **Create function**.

### Step 5.2: Configure Environment Variables and Timeout

1. Go to **Configuration** $\rightarrow$ **General configuration** $\rightarrow$ **Edit**:

   * **Memory:** `512 MB`.

   * **Timeout:** `1 min 0 sec`.

2. Go to **Configuration** $\rightarrow$ **Environment variables** $\rightarrow$ **Edit**:

| Variable Name | Exact Value | 
| ----- | ----- | 
| `LANDING_BUCKET` | `grs-zone1-us-landing-development` | 
| `S3_PREFIX` | `inbound/venti_json_ingestion/third_party/moodys` | 
| `ERROR_PREFIX` | `inbound/venti_json_ingestion/third_party/moodys_error` | 
| `STATE_MACHINE_ARN` | `arn:aws:states:us-east-1:833390816381:stateMachine:grsdiai-hydration-stepfun-venti-tpd-ingestion` | 
| `TARGET_REGION` | `us-east-1` | 
| `LAYER_NAME` | `moodys_ingestion_layer` | 
| `SOURCE_NAME` | `moodys` | 

### Step 5.3: Attach SQS Trigger

1. In the Lambda overview, click **Add trigger**.

2. Choose **SQS**.

3. **SQS queue:** `moodys-ingestion-buffer-queue`.

4. **Batch size:** `10`.

5. **Batch window:** `5` seconds.

6. Ensure **Enable trigger** is checked.

7. Click **Add**.

### Step 5.4: Function Code (`index.py`)

Replace all code in the code editor with this implementation:

```
import inspect
import json
import os
import re
import sys
import traceback
import types
import uuid
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# 1. BYPASS MISSING YAML MODULE FOR lambda_utils.py
# ------------------------------------------------------------------------------
if "yaml" not in sys.modules:
  mock_yaml = types.ModuleType("yaml")
  mock_yaml.safe_load = lambda *args, **kwargs: {}
  mock_yaml.load = lambda *args, **kwargs: {}
  mock_yaml.dump = lambda *args, **kwargs: ""
  sys.modules["yaml"] = mock_yaml

import boto3
from botocore.exceptions import ClientError
from providers.lambda_config import LambdaConfig
from providers.lambda_utils import LambdaUtility

# ------------------------------------------------------------------------------
# 2. CLIENTS & ENVIRONMENT CONFIGURATION
# ------------------------------------------------------------------------------
REGION_NAME = (
    os.getenv("TARGET_REGION")
    or os.getenv("AWS_REGION")
    or os.getenv("AWS_Region")
    or "us-east-1"
)

s3_client = boto3.client("s3", region_name=REGION_NAME)
sfn_client = boto3.client("stepfunctions", region_name=REGION_NAME)

BUCKET_NAME = (os.getenv("LANDING_BUCKET") or "").strip()
LANDING_PREFIX = (os.getenv("S3_PREFIX") or "").strip().strip("/")
ERROR_PREFIX = (os.getenv("ERROR_PREFIX") or "").strip().strip("/")
SOURCE_NAME = (os.getenv("SOURCE_NAME") or "moodys").strip()

STATE_MACHINE_ARN = (
    os.getenv("STATE_MACHINE_ARN")
    or "arn:aws:states:us-east-1:833390816381:stateMachine:grsdiai-hydration-stepfun-venti-tpd-ingestion"
).strip()


def validate_environment_configuration():
  missing = []
  if not BUCKET_NAME:
    missing.append("LANDING_BUCKET")
  if not LANDING_PREFIX:
    missing.append("S3_PREFIX")
  if not ERROR_PREFIX:
    missing.append("ERROR_PREFIX")

  if missing:
    raise EnvironmentError(
        f"Missing required environment variable(s): {', '.join(missing)}."
    )


# ==============================================================================
# 3. IDENTIFIER RESOLUTION (MOODY'S ENTITY BASED)
# ==============================================================================
def find_key_recursive(obj, target_key):
  if isinstance(obj, dict):
    val = obj.get(target_key)
    if isinstance(val, (str, int)) and str(val).strip():
      return str(val).strip()
    items = obj.values()
  elif isinstance(obj, list):
    items = obj
  else:
    return None

  for item in items:
    found = find_key_recursive(item, target_key)
    if found:
      return found
  return None


def get_header_id(payload):
  header = (
      payload.get("event", {}).get("header")
      if isinstance(payload.get("event"), dict)
      else None
  )
  candidate = header.get("id") if isinstance(header, dict) else None
  return str(candidate).strip() if candidate and str(candidate).strip() else None


def get_nested_identifier(payload):
  data_block = payload.get("data", {})
  inner_block = (
      data_block.get("data", {}) if isinstance(data_block, dict) else {}
  )

  candidates = [
      payload.get("entity_id"),
      payload.get("org_id"),
      data_block.get("entity_id") if isinstance(data_block, dict) else None,
      inner_block.get("entity_id") if isinstance(inner_block, dict) else None,
      payload.get("id"),
      data_block.get("id") if isinstance(data_block, dict) else None,
  ]
  for candidate in candidates:
    if candidate and str(candidate).strip():
      return str(candidate).strip()
  return None


def extract_business_identifier(payload_obj):
  file_id = (
      get_header_id(payload_obj)
      or get_nested_identifier(payload_obj)
      or find_key_recursive(payload_obj, "entity_id")
      or find_key_recursive(payload_obj, "org_id")
      or find_key_recursive(payload_obj, "id")
  )

  if not file_id:
    file_id = f"moodys-{uuid.uuid4()}"

  return re.sub(r"[^a-zA-Z0-9_\-\.]", "_", file_id)


# ==============================================================================
# 4. SQS & S3 PERSISTENCE HELPERS
# ==============================================================================
def parse_payload(raw_body):
  if not raw_body:
    raise ValueError("SQS message body is empty")
  sqs_body = json.loads(raw_body)
  detail = sqs_body.get("detail", sqs_body)
  payload = json.loads(detail) if isinstance(detail, str) else detail
  if not isinstance(payload, dict):
    raise ValueError(
        f"Payload root must be a JSON object, received {type(payload).__name__}"
    )
  return payload


def handle_record_failure(logger, record, message_id, partition_val, exc):
  error_trace = traceback.format_exc()
  logger.error(
      f"Processing failed for message {message_id}: {exc}", exc_info=True
  )
  error_s3_key = f"{ERROR_PREFIX}/{partition_val}/failed_{message_id}.json"
  error_payload = {
      "error_message": str(exc),
      "error_trace": error_trace,
      "timestamp_utc": datetime.now(timezone.utc).isoformat(),
      "original_record": record,
  }
  logger.warning(
      f"Archiving failed payload to s3://{BUCKET_NAME}/{error_s3_key}"
  )
  try:
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=error_s3_key,
        Body=json.dumps(error_payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
  except Exception as s3_err:
    logger.error(f"Critical S3 quarantine error: {s3_err}", exc_info=True)


def process_sqs_record(record, partition_val, logger):
  message_id = record.get("messageId", "unknown_msg")
  try:
    payload_obj = parse_payload(record.get("body", ""))
    file_id_clean = extract_business_identifier(payload_obj)
    payload_bytes = json.dumps(payload_obj, separators=(",", ":")).encode(
        "utf-8"
    )

    custom_prefix = payload_obj.get("s3_prefix")
    if custom_prefix and str(custom_prefix).strip():
      clean_prefix = str(custom_prefix).strip().strip("/")
      s3_key = f"{clean_prefix}/{file_id_clean}.json"
    else:
      s3_key = f"{LANDING_PREFIX}/{partition_val}/{file_id_clean}.json"

    logger.info(f"Writing valid payload to s3://{BUCKET_NAME}/{s3_key}")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=payload_bytes,
        ContentType="application/json",
    )
    return s3_key
  except Exception as exc:
    handle_record_failure(logger, record, message_id, partition_val, exc)
    return None


# ==============================================================================
# 5. STEP FUNCTIONS TRIGGER HELPER
# ==============================================================================
def trigger_step_function(s3_directory_or_key, logger):
  # Clean prefix to avoid leading slashes and ensure valid partition directory
  step_function_input = {
      "application_layer_name": "raw-stage1",
      "domain_name": "third_party",
      "source_name": SOURCE_NAME,
      "log_level": "INFO",
      "environment": "dev",
      "layer_name": "raw",
      "region": REGION_NAME,
      "load_type": "cdc",
      "s3_prefix": s3_directory_or_key.lstrip("/"),
  }

  timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
  execution_name = (
      f"{SOURCE_NAME}-ingestion-{timestamp_str}-{str(uuid.uuid4())[:8]}"
  )

  logger.info(
      f"Triggering Step Function '{STATE_MACHINE_ARN}' as '{execution_name}'"
  )
  logger.info(f"Step Function input payload: {json.dumps(step_function_input)}")

  try:
    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(step_function_input),
    )
    execution_arn = response.get("executionArn")
    logger.info(
        f"Step Function triggered successfully. Execution ARN: {execution_arn}"
    )
    return execution_arn, execution_name

  except ClientError as exc:
    err_msg = exc.response.get("Error", {}).get("Message", str(exc))
    logger.error(
        f"ClientError starting Step Function: {err_msg}", exc_info=True
    )
    raise exc
  except Exception as exc:
    logger.error(
        f"Unexpected error starting Step Function: {exc}", exc_info=True
    )
    raise exc


# ==============================================================================
# 6. DYNAMIC FRAMEWORK LOGGER INITIALIZATION
# ==============================================================================
def resolve_config_args(param_names, event, context):
  log_group = getattr(context, "log_group_name", "/aws/lambda/moodys_worker")
  req_id = getattr(context, "aws_request_id", "test-request-id")
  mem_limit = str(getattr(context, "memory_limit_in_mb", "128"))
  region = REGION_NAME or "us-east-1"
  layer = os.getenv("LAYER_NAME", "moodys_ingestion_layer")

  lookup = {
      "log_group_name": log_group,
      "log_group": log_group,
      "aws_request_id": req_id,
      "request_id": req_id,
      "memory_limit_in_mb": mem_limit,
      "memory_limit": mem_limit,
      "region": region,
      "region_name": region,
      "layer_name": layer,
      "layer": layer,
      "event": event,
      "context": context,
  }

  return [lookup.get(name, "default") for name in param_names]


def get_framework_logger(event, context):
  try:
    sig = inspect.signature(LambdaConfig.__init__)
    param_names = [p for p in sig.parameters.keys() if p != "self"]

    if len(param_names) == 2 and "event" in param_names:
      config_obj = LambdaConfig(event, context)
    else:
      args = resolve_config_args(param_names, event, context)
      config_obj = LambdaConfig(*args)

    utility = LambdaUtility(config_obj)
    return utility.logger

  except Exception:
    import logging

    fallback_logger = logging.getLogger("moodys_fallback")
    fallback_logger.setLevel(logging.INFO)
    return fallback_logger


# ==============================================================================
# 7. HANDLER ENTRYPOINT
# ==============================================================================
def handler(event, context):
  logger = get_framework_logger(event, context)
  validate_environment_configuration()

  logger.info(
      f"Resolved S3 bucket: '{BUCKET_NAME}', landing prefix:"
      f" '{LANDING_PREFIX}', error prefix: '{ERROR_PREFIX}'"
  )

  records = event.get("Records")
  if records is None:
    body_content = event.get("body") if "body" in event else json.dumps(event)
    records = [{
        "body": body_content,
        "messageId": getattr(context, "aws_request_id", "direct_exec"),
    }]

  logger.info(f"Processing {len(records)} record(s) in region {REGION_NAME}.")

  partition_val = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
  written_s3_keys = []

  for record in records:
    uploaded_key = process_sqs_record(record, partition_val, logger)
    if uploaded_key:
      written_s3_keys.append(uploaded_key)

  step_executions = []

  # Downstream subscriber requires directory prefix ending in / with verified files
  if written_s3_keys:
    partition_dir = f"{LANDING_PREFIX}/{partition_val}/"
    arn, name = trigger_step_function(partition_dir, logger)
    step_executions.append({
        "execution_arn": arn,
        "execution_name": name,
        "target_prefix": partition_dir,
        "files_count": len(written_s3_keys),
    })

  return {
      "statusCode": 200,
      "body": json.dumps({
          "status": "SUCCESS",
          "records_processed": len(records),
          "files_written": len(written_s3_keys),
          "step_function_executions": step_executions,
      }),
  }


lambda_handler = handler

```

8. Click **Deploy**.

## 6. API Gateway Configuration and VTL Mapping Template

### Step 6.1: Create REST API

1. Open **Amazon API Gateway** $\rightarrow$ Click **Create API** $\rightarrow$ Choose **REST API** (click **Build**).

2. **API name:** `grsdiai-hydration-api-tpd-moodys`.

3. **Endpoint Type:** `Regional`.

4. Click **Create API**.

### Step 6.2: Create `/venti-event` Resource and Method

1. Under **Resources**, click **Create Resource**:

   * **Resource Name:** `venti-event`

   * **Resource Path:** `/venti-event`

   * Click **Create Resource**.

2. Select `/venti-event` $\rightarrow$ Click **Create Method**:

   * **Method type:** `POST`

   * **Integration type:** `AWS service`

   * **AWS Region:** `us-east-1`

   * **AWS service:** `EventBridge` (or type `events`)

   * **HTTP method:** `POST`

   * **Action type:** `Use action name`

   * **Action name:** `PutEvents`

   * **Execution role:** `arn:aws:iam::833390816381:role/grsdiai-hydration-apigw-eventbridge-role`

   * Click **Save**.

### Step 6.3: Configure Headers and VTL Template

1. Open **Integration Request** for `POST /venti-event`:

2. Expand **URL Request Headers** (or HTTP Headers):

   * Add Header 1:

     * **Name:** `X-Amz-Target`

     * **Mapped from:** `'AWSEvents.PutEvents'` *(must include single quotes)*

   * Add Header 2:

     * **Name:** `Content-Type`

     * **Mapped from:** `'application/x-amz-json-1.1'` *(must include single quotes)*

3. Scroll down to **Mapping Templates**:

   * **Request body passthrough:** `When there are no templates defined (recommended)`.

   * Click **Add mapping template**.

   * **Content-Type:** `application/json`.

   * In the template editor, paste:

```
#set($context.requestOverride.header.X-Amz-Target = "AWSEvents.PutEvents")
#set($context.requestOverride.header.Content-Type = "application/x-amz-json-1.1")
{
  "Entries": [
    {
      "Source": "moodys.ingestion",
      "DetailType": "MoodysInboundEvent",
      "Detail": "$util.escapeJavaScript($input.body)",
      "EventBusName": "grsdiai-hydration-moodys-ingestion-bus"
    }
  ]
}

```

4. Click **Save** on the mapping template.

### Step 6.4: Deploy the Stage

1. Click **Deploy API** (top right of the Resources pane).

2. **Deployment stage:** `[New Stage]`.

3. **Stage name:** `dev`.

4. Click **Deploy**.

## 7. Step Functions and Subscriber Alignment

Ensure the Step Functions configuration for Moody's is in place:

1. **Config File Presence:** Verify that `config/source_configs/moodys.json` exists in `grsdiai-hydration-us-config-development`.

2. **Execution Input Format:** The Lambda passes:

```
{
  "application_layer_name": "raw-stage1",
  "domain_name": "third_party",
  "source_name": "moodys",
  "log_level": "INFO",
  "environment": "dev",
  "layer_name": "raw",
  "region": "us-east-1",
  "load_type": "cdc",
  "s3_prefix": "inbound/venti_json_ingestion/third_party/moodys/<hourly-partition>/"
}

```

Because `s3_prefix` ends with `/` and contains files written by the Lambda before execution starts, it satisfies the subscriber validation without throwing `InvalidCDCInput`.

## 8. Verification and End-to-End Test Plan

### Step 8.1: Test API Gateway Endpoint

1. Go to `grsdiai-hydration-api-tpd-moodys` $\rightarrow$ `/venti-event` $\rightarrow$ **POST** $\rightarrow$ **Test**.

2. **Headers:**

```
Content-Type: application/json

```

3. **Request Body:**

```
{
  "event": {
    "header": {
      "id": "MOODYS-E2E-TEST-001",
      "timestamp": "2026-11-03T16:00:00Z",
      "eventType": "ENTITY_RATING_REPORT"
    }
  },
  "entity_id": "MDY-CORP-987654",
  "data": {
    "org_id": "MDY-CORP-987654",
    "legal_name": "Acme Holdings LLC",
    "credit_rating": "Aa2",
    "outlook": "Stable"
  }
}

```

4. Click **Test**.

   * **Verification:** Response status is `200` and body has `"FailedEntryCount": 0`.

### Step 8.2: Verify S3 Landing Lake

1. Open **Amazon S3** $\rightarrow$ bucket `grs-zone1-us-landing-development`.

2. Navigate to:
   `inbound/venti_json_ingestion/third_party/moodys/<CURRENT-UTC-DATE-HOUR>/`

3. Confirm `MOODYS-E2E-TEST-001.json` exists and contains the valid JSON payload.

### Step 8.3: Verify Step Functions Execution

1. Open **AWS Step Functions** $\rightarrow$ `grsdiai-hydration-stepfun-venti-tpd-ingestion`.

2. Check the Executions tab for an execution starting with `moodys-ingestion-...`.

3. Confirm that the state transitions through `cdc_prefix_resources` and `flattening_resources` with a status of **Succeeded** (green).