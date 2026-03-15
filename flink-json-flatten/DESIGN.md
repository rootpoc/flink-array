# Flink JSON Flatten Pipeline — Design

## Overview

Reads JSON messages from Kafka, validates and flattens them into a single named
Flink `Row`, applies field transformations on the **complete unsplit record**,
then optionally splits into pages before writing to Kafka with exactly-once guarantees.

---

## Pipeline Topology

```
KafkaSource (input-topic, byte[])
      │
      ▼
ValidateFlattenFunction ──side-output──► DLQ KafkaSink (dlq-topic)
      │  ProcessedMessage — full Row, ALL array elements present
      ▼
UpperCaseMapFunction
      │  ProcessedMessage — all matching string fields uppercased across every array element
      ▼  (only when processing.split-enabled=true)
SplitFunction  (splits on processing.split-field)
      │  ProcessedMessage × N pages — items re-indexed 0…(pageSize-1) per page
      ▼
KafkaSink (output-topic, exactly-once)
```

---

## Steps

### Step 1 — Kafka Source
- Reads raw `byte[]` messages from the configured input Kafka topic.
- Deserializes via `KafkaEnvelopeDeserializer` → `ProcessedMessage`.
- `isolation.level=read_committed` skips uncommitted messages (EOS consumer side).
- Starts from earliest committed offsets.

---

### Step 2 — `ValidateFlattenFunction`
Validates required fields then flattens the **entire** JSON tree into one `Row.withNames()`.

| Sub-step | What happens |
|---|---|
| **Validate** | Checks required top-level fields against `input-netflix-categories.schema.json`. Failures → DLQ. |
| **Flatten** | Converts the full nested JSON (including all array elements) into dot-notation `Row` keys, e.g. `search_engines.0.imdb.director`. No splitting is performed here. |

The full unsplit Row is emitted so downstream operators can process **all array
elements in one pass**.

#### Step 2a — DLQ Side Output
Validation errors and parse failures are serialized via `DlqSerializationSchema`
and written to the DLQ Kafka topic.

---

### Step 3 — `UpperCaseMapFunction`
Iterates **every field name** in the full Row and uppercases string values whose
name matches any configured pattern in `processing.uppercase-field-keys`.

| Key behaviour | Detail |
|---|---|
| **Full row scope** | Operates before split, so all `search_engines.N.imdb.director` keys are transformed in one map call. |
| **Wildcard patterns** | `search_engines.*.imdb.director` matches every index. |
| **No re-processing per page** | Because this runs pre-split, every generated page inherits already-transformed values automatically. |

---

### Step 3a — `SplitFunction` _(conditional)_

Activated only when `processing.split-enabled=true`.

| Property | CLI flag | Default | Description |
|---|---|---|---|
| Enable/disable | `processing.split-enabled` | `true` | Set to `false` to skip splitting entirely |
| Field to split | `processing.split-field` | `search_engines` | Top-level Row array key to paginate |
| Page size | `splitting.page-size` | `100` | Maximum items per output page |

**When enabled:**
- Scans the flat Row for keys matching `{splitField}.N.*`.
- Emits one `ProcessedMessage` per page of `pageSize` items.
- Items within each page are re-indexed from 0.
- Shared fields (top-level scalars, nested objects outside the split field) are copied to every page.
- Pagination metadata (`index`, `total`, `count`) is added using field names from `PaginationSchema`.

**When disabled:**
- `processedStream` flows directly to the sink — no `SplitFunction` operator is added to the topology.

---

### Step 4 — Kafka Sink (Exactly-Once)
- Serializes each `ProcessedMessage` back to hierarchical JSON bytes via `ReconstructSerializer`.
- Writes to the output Kafka topic using Kafka transactions (`transactionalIdPrefix` + EOS producer).
- `transaction.timeout.ms` must exceed checkpoint interval + timeout.

---

## Exactly-Once Guarantee

| Layer | Mechanism |
|---|---|
| Source | `isolation.level=read_committed` |
| Engine | Flink checkpointing (`EXACTLY_ONCE`, interval-based) |
| Sink | Kafka transactions via `transactionalIdPrefix` |

---

## Configuration Reference

| CLI flag | Default | Description |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | Kafka broker list |
| `kafka.input-topic` | `input-topic` | Source topic |
| `kafka.output-topic` | `output-topic` | Sink topic |
| `kafka.dlq-topic` | `dlq-topic` | Dead-letter topic |
| `kafka.consumer-group` | `flink-json-flatten-cg` | Consumer group ID |
| `job.parallelism` | `4` | Flink operator parallelism |
| `checkpoint.interval-ms` | `60000` | Checkpoint interval |
| `checkpoint.timeout-ms` | `120000` | Checkpoint timeout |
| `checkpoint.storage` | `file:///tmp/…` | State backend URI |
| `processing.split-enabled` | `true` | Enable/disable page splitting |
| `processing.split-field` | `search_engines` | Row array field to split on |
| `splitting.page-size` | `100` | Items per page |
| `flatten.null-handling` | `INCLUDE` | `INCLUDE` / `EXCLUDE` / `REPLACE_EMPTY_STRING` |
| `processing.uppercase-field-keys` | `person.name,name` | Comma-separated field patterns to uppercase |
| `processing.third-party-jar` | _(empty)_ | Vendor JAR path (falls back to `toUpperCase()`) |

---

## Class Map

| Class | Package | Role |
|---|---|---|
| `JsonFlattenPipeline` | `com.pipeline` | Entry point — topology wiring |
| `ValidateFlattenFunction` | `com.pipeline` | Step 2 — validate + full-tree flatten + DLQ side output |
| `UpperCaseMapFunction` | `com.pipeline.processing` | Step 3 — field transformation on full unsplit Row |
| `SplitFunction` | `com.pipeline.splitting` | Step 3a — conditional page splitting post-transform |
| `ValidateSplitFlattenFunction` | `com.pipeline` | _(legacy)_ combined validate+split+flatten in one pass |
| `ReconstructSerializer` | `com.pipeline.serialization` | Row → JSON bytes for Kafka sink |
| `KafkaEnvelopeDeserializer` | `com.pipeline.deserialization` | `byte[]` → `ProcessedMessage` for Kafka source |
| `DlqSerializationSchema` | `com.pipeline.serialization` | `DlqRecord` → bytes for DLQ sink |
| `PipelineConfig` | `com.pipeline.config` | Parsed CLI parameters |
| `SchemaAnalyzer` | `com.pipeline.splitting.schema` | Loads and analyzes JSON schemas |
| `PageBuilder` | `com.pipeline.splitting.page` | Builds a single JSON page `ObjectNode` |
