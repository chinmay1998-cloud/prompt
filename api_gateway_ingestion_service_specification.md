# Component Specification: Amazon API Gateway

## 1. Overview & Purpose
Amazon API Gateway serves as the public entry point and edge perimeter for the Carfax Data Ingestion pipeline. It exposes a secure HTTP endpoint (`/venti-event`) designed to receive webhook pushes from upstream clients and internal microservices. 

Instead of routing traffic directly to compute infrastructure, API Gateway utilizes an asynchronous direct service integration with **Amazon EventBridge**, achieving sub-millisecond decoupling, zero ingestion bottlenecks, and an immediate acknowledgment back to the producer.

---

## 2. Service Configuration & Endpoint Details

| Parameter | Value |
| :--- | :--- |
| **API Name** | `grsdiai-hydration-api-carfax-ingestion` |
| **API Type** | REST API (Edge-Optimized / Regional) |
| **Resource Path** | `/venti-event` |
| **HTTP Method** | `POST` |
| **Integration Type** | AWS Service Integration (`EventBridge / events:PutEvents`) |
| **Stage** | `development` / `production` |
| **Expected Headers** | `Content-Type: application/json`, `x-partition-date: YYYY-MM-DD` |

---

## 3. Integration & Request Transformation

The integration bypasses compute and pushes events directly onto the custom EventBridge bus.

### Integration Request Mapping Template (`application/json`)
```json
#set($context.requestOverride.header.X-Amz-Target = "AWSEvents.PutEvents")
#set($context.requestOverride.header.Content-Type = "application/x-amz-json-1.1")
{
  "Entries": [
    {
      "Source": "venti.api.gateway",
      "DetailType": "venti-carfax-ingestion",
      "EventBusName": "arn:aws:events:us-east-1:123456789012:event-bus/grsdiai-hydration-eventbus-carfax",
      "Detail": "$util.escapeJavaScript($input.body)"
    }
  ]
}
```

### Integration Response & Client ACK
* **Success Status Code:** `200 OK` or `201 Created`
* **Response Body Sent to Client:**
```json
{
  "status": "ACCEPTED",
  "requestId": "$context.requestId",
  "message": "Carfax ingestion event queued successfully."
}
```

---

## 4. IAM Execution Role & Policies

API Gateway requires an assumed IAM execution role with permissions to emit events onto the target EventBridge bus.

### Trust Relationship
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "apigateway.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### IAM Permissions Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPutEventsToCarfaxBus",
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "arn:aws:events:us-east-1:123456789012:event-bus/grsdiai-hydration-eventbus-carfax"
    }
  ]
}
```

---

## 5. Operational Verification & Troubleshooting

### Client Verification (`curl`)
```bash
curl -X POST https://<api-id>.execute-api.us-east-1.amazonaws.com/development/venti-event \
  -H "Content-Type: application/json" \
  -H "x-partition-date: 2026-10-23" \
  -d '{
    "event": {
      "header": { "id": "TEST_EVT_1001" }
    },
    "vin": "1HGCR2F8XHA000000",
    "data": { "mileage": 42150 }
  }'
```

### Edge Failure Distinction (4xx / 5xx)
* **Critical Principle:** Requests rejected at API Gateway (HTTP `400`, `401`, `403`, `429`, or `500`) **never reach EventBridge, SQS, or Lambda**.
* They do not land in the S3 `carfax_error/` folder.
* **Troubleshooting Logs:** Enable Execution Logging on the stage. Logs appear under:
  ```text
  API-Gateway-Execution-Logs_<api-id>/<stage-name>
  ```