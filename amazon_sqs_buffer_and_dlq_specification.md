# Component Specification: Amazon SQS (Buffer & DLQ)

## 1. Overview & Purpose
Amazon Simple Queue Service (SQS) acts as the resilient traffic buffer and safety net of the Carfax ingestion architecture. It isolates upstream webhook delivery from downstream Lambda compute, ensuring that traffic spikes do not exceed concurrency limits or overwhelm downstream databases.

The architecture employs two distinct FIFO queues:
1. **Primary FIFO Buffer Queue:** Buffers messages, enforces strict ordering, deduplicates events, and triggers the Lambda worker.
2. **Dead-Letter Queue (FIFO DLQ):** Captures poisonous messages and unhandled runtime crashes (Lambda timeouts, memory exhaustion) that exceed retry limits.

---

## 2. Queue Configuration Details

| Parameter | Primary Buffer Queue | Dead-Letter Queue (DLQ) |
| :--- | :--- | :--- |
| **Queue Name** | `grsdiai-hydration-sqs-carfax-ingestion-buffer.fifo` | `grsdiai-hydration-sqs-carfax-ingestion-buffer-dlq.fifo` |
| **Queue Type** | FIFO | FIFO |
| **Message Retention** | 4 Days (345,600 seconds) | 14 Days (1,209,600 seconds) |
| **Visibility Timeout** | 300 seconds (5 minutes) | 300 seconds |
| **Delivery Delay** | 0 seconds | 0 seconds |
| **Content Deduplication**| Enabled | Enabled |
| **Redrive Policy** | Attached (Points to DLQ) | N/A |
| **`maxReceiveCount`** | `3` (or `5`) | N/A |

---

## 3. Redrive Policy & DLQ Behavior

```text
[EventBridge] ──► [Primary SQS FIFO] ──────────► [Lambda Worker]
                         │                              │
                         │ (Retried if no 200 returned) │ (Crashes / OOM / Timeout)
                         │◄─────────────────────────────┘
                         │
             (If ReceiveCount > 3)
                         │
                         ▼
                [SQS FIFO DLQ]
```

### Redrive Policy JSON (Attached to Buffer Queue)
```json
{
  "deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:grsdiai-hydration-sqs-carfax-ingestion-buffer-dlq.fifo",
  "maxReceiveCount": 3
}
```

### Critical Architectural Distinction: DLQ vs. S3 Error Folder
* **S3 Quarantine (`carfax_error/`):** Handled business exceptions (missing `vin`, corrupt JSON). Lambda catches these in code, saves an audit package to S3, and acknowledges SQS with a `200`. **These records do not go to the DLQ.**
* **SQS DLQ (`...-dlq.fifo`):** Reserved exclusively for **unhandled infrastructure failures** where the Lambda crashes, runs out of memory, or exceeds its timeout limit without returning an acknowledgment.

---

## 4. SQS Queue Policy (Resource-Based)

Allows EventBridge to push messages into the primary buffer queue.

```json
{
  "Version": "2012-10-17",
  "Id": "AllowEventBridgeToSQSPolicy",
  "Statement": [
    {
      "Sid": "AllowEventBridgeTargetSendMessage",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:123456789012:grsdiai-hydration-sqs-carfax-ingestion-buffer.fifo",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:us-east-1:123456789012:rule/grsdiai-hydration-eventbus-carfax/grsdiai-hydration-rule-carfax-ingestion"
        }
      }
    }
  ]
}
```

---

## 5. Operations & Metrics Guide

### CloudWatch Metric Interpretation
* **`ApproximateNumberOfMessagesVisible`**: Messages waiting in queue. During healthy steady-state processing, this should hover near 0.
* **`NumberOfMessagesReceived` vs. S3 Object Counts**:
  * Lambda consumes in batches (e.g., 5–10 messages per poll).
  * A graph showing 300 receive operations can easily account for 900+ processed files. Always use S3 object counts as the single source of truth for processed records.
* **`ApproximateNumberOfMessagesVisible` on DLQ**: Must be strictly **`0`**. Any count $>0$ indicates an uncaught runtime bug or infrastructural timeout.

### DLQ Redrive & Purge
1. **Purging Old DLQ Records (Testing phase):**
   ```bash
   aws sqs purge-queue \
     --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/grsdiai-hydration-sqs-carfax-ingestion-buffer-dlq.fifo
   ```
2. **Redriving DLQ Records to Source:** Use the AWS SQS Console $\to$ **DLQ redrive** to replay unprocessed messages back into the primary FIFO queue once code patches are deployed.