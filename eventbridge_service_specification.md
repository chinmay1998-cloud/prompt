# Component Specification: Amazon EventBridge

## 1. Overview & Purpose
Amazon EventBridge operates as the central routing backbone of the ingestion architecture. It receives event envelopes emitted by Amazon API Gateway, enforces routing rules based on event source and detail-type filters, and forwards valid payloads to the downstream FIFO queue.

By serving as an event bus, EventBridge isolates API Gateway from the underlying queuing mechanisms, allowing additional downstream subscribers (e.g., audit consumers, analytics engines) to be added without impacting core ingestion.

---

## 2. Resource Details

| Property | Value |
| :--- | :--- |
| **Bus Name** | `grsdiai-hydration-eventbus-carfax` |
| **Type** | Custom Event Bus |
| **Rule Name** | `grsdiai-hydration-rule-carfax-ingestion` |
| **Target** | Amazon SQS FIFO Queue (`...-buffer.fifo`) |
| **Dead-Letter Option** | Enabled at Target level for failed routing invocations |

---

## 3. Event Envelope Structure & Routing Rules

API Gateway envelopes client payloads into a standard AWS EventBridge JSON schema.

### Sample Event Envelope
```json
{
  "version": "0",
  "id": "c1f7b8a2-3b4a-4b9a-bc2d-123456789abc",
  "detail-type": "venti-carfax-ingestion",
  "source": "venti.api.gateway",
  "account": "123456789012",
  "time": "2026-10-23T07:30:00Z",
  "region": "us-east-1",
  "resources": [],
  "detail": {
    "event": {
      "header": {
        "id": "CARFAX_EVT_1001"
      }
    },
    "vin": "1HGCR2F8XHA000000",
    "data": {
      "mileage": 42150
    }
  }
}
```

### Event Pattern (Routing Rule)
The rule matches on the exact `source` and `detail-type`:
```json
{
  "source": ["venti.api.gateway"],
  "detail-type": ["venti-carfax-ingestion"]
}
```

---

## 4. Target Configuration & Message Grouping (FIFO)

Because the downstream consumer is an SQS FIFO queue, EventBridge must specify a `MessageGroupId`:
* **Target ARN:** `arn:aws:sqs:us-east-1:123456789012:grsdiai-hydration-sqs-carfax-ingestion-buffer.fifo`
* **Message Group ID Path:** Set to `$.detail.event.header.id` or constant `"default"` to maintain deterministic ordering.
* **Message Deduplication ID:** Inherited from EventBridge `$.id` (UUID).

---

## 5. IAM Target Execution Role

EventBridge requires an IAM service role to publish messages into the target SQS FIFO queue.

### Trust Relationship
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Permissions Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSQSSendMessage",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:us-east-1:123456789012:grsdiai-hydration-sqs-carfax-ingestion-buffer.fifo"
    }
  ]
}
```

---

## 6. CloudWatch Metrics & Monitoring

| Metric | Target | Description |
| :--- | :--- | :--- |
| **`Invocations`** | $> 0$ | Total number of events delivered to the EventBus. |
| **`MatchedEvents`** | Equal to `Invocations` | Number of events matching `grsdiai-hydration-rule-carfax-ingestion`. |
| **`TriggeredRules`** | Equal to `MatchedEvents` | Confirmations that the rule triggered the SQS target. |
| **`FailedInvocations`** | **`0`** | Must strictly remain 0. Values $>0$ indicate IAM permission issues or SQS throttling. |