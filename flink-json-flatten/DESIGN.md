# Flink Flatten Pipeline — Design

## Overview

The pipeline reads raw Kafka messages as `byte[]`, wraps them in a `ProcessedMessage`,
selects the configured **input `ProcessFunction`** (`JSON` or `CSV`) to build a
named Flink `Row`, applies field transformations on the **complete unsplit row**,
optionally splits array content into pages, then selects the configured **output
`ProcessFunction`** (`JSON` or `CSV`) to serialize the row back to bytes before
writing to Kafka with exactly-once guarantees.

The current runtime supports:
- **JSON input** → validate against JSON Schema, then flatten into dot-notation `Row`
- **CSV input** → parse ordered mandatory columns into `Row`
- **JSON output** → reconstruct hierarchical JSON bytes from `Row`
- **CSV output** → serialize ordered CSV bytes from `Row`

---

## Pipeline Topology

```text
KafkaSource (input-topic, byte[])
      │
      ▼
KafkaEnvelopeDeserializer
      │  byte[] -> ProcessedMessage(originalBytes, kafkaKey, headers, payload=null)
      ▼
InputProcessFunctionFactory.create(config) ──side-output──► DLQ KafkaSink (dlq-topic)
      │  JSON -> JsonInputProcessFunction
      │  CSV  -> CsvInputProcessFunction
      │  ProcessedMessage(payload=Row)
      ▼
UpperCaseMapFunction
      │  ProcessedMessage(payload=transformed Row)
      ▼  (only when processing.split-enabled=true)
SplitFunction
      │  ProcessedMessage x N pages (payload=page Row)
      ▼
OutputProcessFunctionFactory.create(config) ──side-output──► DLQ KafkaSink (dlq-topic)
      │  JSON -> JsonOutputProcessFunction
      │  CSV  -> CsvOutputProcessFunction
      │  SerializedMessage(value=final bytes)
      ▼
KafkaSink (output-topic, exactly-once)
```

---

## Message Model

### `ProcessedMessage`
Carries Kafka envelope metadata and the business payload while the record moves
through input, transform, and split stages.

Fields:
- `kafkaKey: byte[]`
- `originalBytes: byte[]`
- `headers: Map<String, byte[]>`
- `payload: Row`

Lifecycle:
1. `KafkaEnvelopeDeserializer` creates `ProcessedMessage` with `payload = null`
2. Input `ProcessFunction` parses `originalBytes` and attaches `payload = Row`
3. Transform/split stages replace `payload` with transformed or paged `Row`
4. Output `ProcessFunction` reads `payload` and produces final serialized bytes

### `SerializedMessage`
Represents the final Kafka-ready output from the configured output stage.

Fields:
- `kafkaKey: byte[]`
- `value: byte[]`
- `headers: Map<String, byte[]>`

---

## Runtime Steps

### Step 1 — Kafka Source
- Reads raw `byte[]` messages from the configured input Kafka topic
- Uses `KafkaEnvelopeDeserializer`
- Preserves the Kafka key and headers in `ProcessedMessage`
- Sets `payload = null` at the source
- Uses `isolation.level=read_committed`

---

### Step 2 — Input `ProcessFunction` Selection
Runtime input handling is chosen from configuration:
- `input.format=json` -> `JsonInputProcessFunction`
- `input.format=csv`  -> `CsvInputProcessFunction`

Selection is done by `InputProcessFunctionFactory.create(config)`.

#### Step 2a — `JsonInputProcessFunction`
- Loads the configured input JSON schema from `input.schema-resource`
- Analyzes it with `InputSchemaInfo.analyze(...)`
- Parses the incoming `originalBytes` into `JsonNode`
- Validates required fields
- Flattens the entire JSON tree into `Row.withNames()`
- Uses dot-notation keys such as:
  - `metadata.timestamp`
  - `persons.0.firstName`
  - `persons.1.address.city`
- Routes parse or validation failures to the input DLQ side output

**Shared helper logic:**
`ValidateFlattenFunction` still contains reusable JSON validation/flatten helpers,
but it is no longer the live ingress operator in the production topology.

#### Step 2b — `CsvInputProcessFunction`
- Loads the configured CSV schema from `input.schema-resource`
- Uses `CsvDeserializer`
- Derives ordered columns from the schema `required` array via `SchemaAnalyzer.extractCsvColumns(...)`
- Treats CSV fields as mandatory and positional
- Builds a named `Row` from the incoming CSV line
- Routes malformed/short/invalid CSV records to the input DLQ side output

---

### Step 3 — `UpperCaseMapFunction`
Applies field transformation on the **full unsplit row**.

Behavior:
- Iterates all named `Row` fields
- Matches field names against configured patterns in `processing.uppercase-field-keys`
- Uppercases matching string values
- Runs before splitting, so all array elements are transformed in a single pass

Examples:
- `person.name`
- `persons.*.firstName`
- `search_engines.*.imdb.director`

---

### Step 4 — `SplitFunction` (Conditional)
Activated only when `processing.split-enabled=true`.

Uses:
- `processing.split-field`
- `splitting.page-size`
- `output.schema-resource` analyzed into `PaginationSchema`

Behavior:
- Scans the flat row for array-item keys under the configured split field
- Emits one `ProcessedMessage` per page
- Re-indexes items in each page from `0`
- Copies shared non-array fields into every page
- Writes pagination metadata fields defined by `PaginationSchema`

Example paged metadata fields:
- `metadata.page`
- `metadata.totalCount`
- `count`

If splitting is disabled:
- the full transformed row flows directly to the output stage

---

### Step 5 — Output `ProcessFunction` Selection
Runtime output handling is chosen from configuration:
- `output.format=json` -> `JsonOutputProcessFunction`
- `output.format=csv`  -> `CsvOutputProcessFunction`

Selection is done by `OutputProcessFunctionFactory.create(config)`.

#### Step 5a — `JsonOutputProcessFunction`
- Uses `ReconstructSerializer`
- Reads the `Row` payload from `ProcessedMessage`
- Reconstructs hierarchical JSON bytes from dot-notation row fields
- Emits `SerializedMessage`
- Routes output serialization failures to the output DLQ side output

#### Step 5b — `CsvOutputProcessFunction`
- Uses `CsvSerializer`
- Serializes fields in the order defined by the configured CSV schema (`output.schema-resource`)
- Emits `SerializedMessage`
- Routes output serialization failures to the output DLQ side output

---

### Step 6 — Kafka Sink
- Writes `SerializedMessage` to the configured output topic
- Uses `SerializedMessageKafkaRecordSerializationSchema`
- Preserves Kafka key and headers
- Uses Kafka transactions for exactly-once delivery

---

## DLQ Behavior

There are two independent DLQ side outputs:

### Input DLQ
Produced by the selected input `ProcessFunction` when:
- input bytes are empty/null
- JSON parsing fails
- JSON schema validation fails
- CSV parsing fails
- CSV field count does not match schema-defined columns

### Output DLQ
Produced by the selected output `ProcessFunction` when:
- JSON reconstruction fails
- CSV serialization fails
- downstream serializer unexpectedly returns `null`

Both DLQ flows are written with `DlqSerializationSchema` to the configured
`kafka.dlq-topic`.

---

## Exactly-Once Guarantee

| Layer | Mechanism |
|---|---|
| Source | `isolation.level=read_committed` |
| Engine | Flink checkpointing (`EXACTLY_ONCE`) |
| Sink | Kafka transactions via `transactionalIdPrefix` |

Notes:
- `transaction.timeout.ms` must exceed checkpoint interval + timeout
- checkpoint storage is configured via `checkpoint.storage`

---

## Configuration Reference

### Kafka
| CLI flag | Default | Description |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | Kafka broker list |
| `kafka.input-topic` | `input-topic` | Source topic |
| `kafka.output-topic` | `output-topic` | Sink topic |
| `kafka.dlq-topic` | `dlq-topic` | Dead-letter topic |
| `kafka.consumer-group` | `flink-json-flatten-cg` | Consumer group ID |
| `kafka.transaction-prefix` | `flink-json-flatten` | Kafka transactional ID prefix |
| `kafka.transaction-timeout-ms` | `900000` | Kafka transaction timeout |

### Input / Output Format Selection
| CLI flag | Default | Description |
|---|---|---|
| `input.format` / `input_format` | `json` | Selects `JSON` or `CSV` input `ProcessFunction` |
| `input.schema-resource` | `schemas/input-persons-with-metadata.schema.json` | Classpath schema used by the selected input stage |
| `output.format` / `output_format` | `json` | Selects `JSON` or `CSV` output `ProcessFunction` |
| `output.schema-resource` | `schemas/output-persons-paged.schema.json` | Classpath schema used by split pagination and/or CSV output ordering |

### Transform / Split
| CLI flag | Default | Description |
|---|---|---|
| `processing.uppercase-field-keys` | `person.name,name` | Comma-separated field patterns to uppercase |
| `processing.split-enabled` | `true` | Enable/disable post-transform page splitting |
| `processing.split-field` | `persons` | Top-level row array field to paginate |
| `splitting.page-size` | `100` | Maximum items per page |
| `flatten.null-handling` | `INCLUDE` | JSON null handling: `INCLUDE`, `EXCLUDE`, `REPLACE_EMPTY_STRING` |

### Third-Party Processor
| CLI flag | Default | Description |
|---|---|---|
| `processing.third-party-jar` | _(empty)_ | Vendor JAR path (fallback is built-in uppercase) |
| `processing.processor-class` | `com.vendor.StringProcessor` | Vendor processor class |

### Checkpointing / Runtime
| CLI flag | Default | Description |
|---|---|---|
| `job.parallelism` | `4` | Flink operator parallelism |
| `checkpoint.interval-ms` | `60000` | Checkpoint interval |
| `checkpoint.timeout-ms` | `120000` | Checkpoint timeout |
| `checkpoint.min-pause-ms` | `30000` | Minimum pause between checkpoints |
| `checkpoint.unaligned` | `true` | Enable unaligned checkpoints |
| `checkpoint.storage` | `file:///tmp/...` | Checkpoint storage URI |

---

## Class Map

| Class | Package | Role |
|---|---|---|
| `JsonFlattenPipeline` | `com.pipeline` | Entry point and topology wiring |
| `KafkaEnvelopeDeserializer` | `com.pipeline.deserialization` | Kafka `byte[]` -> `ProcessedMessage` envelope |
| `InputProcessFunctionFactory` | `com.pipeline` | Selects JSON or CSV input stage |
| `JsonInputProcessFunction` | `com.pipeline` | JSON parse + validate + flatten -> `Row` |
| `CsvInputProcessFunction` | `com.pipeline` | CSV parse -> `Row` |
| `UpperCaseMapFunction` | `com.pipeline.processing` | Row field transformation |
| `SplitFunction` | `com.pipeline.splitting` | Optional page splitting |
| `OutputProcessFunctionFactory` | `com.pipeline` | Selects JSON or CSV output stage |
| `JsonOutputProcessFunction` | `com.pipeline` | `Row` -> JSON bytes |
| `CsvOutputProcessFunction` | `com.pipeline` | `Row` -> CSV bytes |
| `SerializedMessage` | `com.pipeline.common` | Final Kafka-ready payload container |
| `SerializedMessageKafkaRecordSerializationSchema` | `com.pipeline.serialization` | Writes `SerializedMessage` to Kafka |
| `DlqSerializationSchema` | `com.pipeline.serialization` | Writes `DlqRecord` to Kafka |
| `PipelineConfig` | `com.pipeline.config` | Parsed CLI / runtime configuration |
| `SchemaAnalyzer` | `com.pipeline.splitting.schema` | Loads/analyzes JSON schemas |
| `CsvDeserializer` | `com.pipeline.deserialization` | CSV input parsing |
| `CsvSerializer` | `com.pipeline.serialization` | CSV output serialization |
| `ReconstructSerializer` | `com.pipeline.serialization` | JSON reconstruction from flat `Row` |
| `ValidateFlattenFunction` | `com.pipeline` | Shared JSON validation/flatten helper wrapper; not the main live ingress stage |

---

## Testing Notes

Current round-trip scenario coverage follows the real runtime contract:
- raw input bytes
- `ProcessedMessage`
- configured input `ProcessFunction`
- row assertions
- optional split
- configured output `ProcessFunction`
- final output assertions

This is exercised for:
- JSON input/output (`PersonsSplitRoundTripTest`)
- CSV input/output (`CsvScenarioTest`)
- JSON anyOf variant (`PersonsSplitRoundTripTest` anyOf cases)
