# Flink JSON Flatten Pipeline — Design & Implementation

## Overview

A production-ready Apache Flink 1.20 streaming pipeline that:

1. Consumes raw JSON messages from Kafka
2. Validates required fields, splits large arrays into paginated batches, and flattens nested JSON into a named-field `Row` — all in a **single operator, single JSON parse**
3. Applies string transformations (e.g. uppercase) to selected fields via configurable patterns
4. Reconstructs the `Row` back into hierarchical JSON **or** serializes it as a flat CSV line
5. Publishes results to an output Kafka topic, routing failures to a Dead Letter Queue (DLQ)

---

## Pipeline Topology

```
KafkaSource (input-topic, byte[])
      │
      ▼
ValidateSplitFlattenFunction          ──side-output──► DLQ KafkaSink (dlq-topic)
(ProcessFunction<ProcessedMessage, ProcessedMessage>)
1. Validate required top-level fields against input schema
2. Validate required fields in every array item
3. Split input array into pages of configurable size
4. Flatten each page → Row.withNames() (dot-notation keys)
      │ ProcessedMessage (Row payload)
      ▼
UpperCaseMapFunction
(MapFunction<ProcessedMessage, ProcessedMessage>)
Applies string processor to fields matching configured patterns
      │ ProcessedMessage (mutated Row)
      ▼
ReconstructSerializer  ── OR ──  CsvSerializer
Row → hierarchical JSON          Row → flat CSV line
      │
      ▼
KafkaSink (output-topic, EXACTLY_ONCE)
```

**Single-parse design:** The former three-operator chain (`MessageValidatorFunction` → `ArraySplitterFunction` → `FlatteningDeserializer`) has been merged into `ValidateSplitFlattenFunction`. Input JSON is parsed once; each page is flattened directly into a `Row` with no intermediate `byte[]` round-trip.

---

## Source Files

### Entry Point

| File | Description |
|------|-------------|
| `com.pipeline.JsonFlattenPipeline` | `main()` entry point; wires all operators; configures Kafka source/sink and checkpointing |

### Configuration

| File | Description |
|------|-------------|
| `com.pipeline.config.PipelineConfig` | Immutable configuration holder built from `ParameterTool` (CLI args / properties file). Builder pattern. All parameters have sensible defaults. |

Key config keys:

| CLI key | Default | Description |
|---------|---------|-------------|
| `kafka.bootstrap-servers` | `localhost:9092` | Kafka brokers |
| `kafka.input-topic` | `input-topic` | Source topic |
| `kafka.output-topic` | `output-topic` | Sink topic |
| `kafka.dlq-topic` | `dlq-topic` | Dead letter queue topic |
| `job.parallelism` | `4` | Operator parallelism |
| `checkpoint.interval-ms` | `60000` | Checkpoint interval |
| `processing.third-party-jar` | _(blank)_ | Path to vendor processor JAR |
| `processing.uppercase-field-keys` | `person.name,name` | Comma-separated field patterns to uppercase |
| `splitting.page-size` | `100` | Max items per output page |
| `flatten.null-handling` | `INCLUDE` | `INCLUDE` / `EXCLUDE` / `REPLACE_EMPTY_STRING` |

---

### Common Model

#### `com.pipeline.common.ProcessedMessage`

Envelope that carries Kafka metadata alongside the business payload (`Row`) through every operator:

| Field | Description |
|-------|-------------|
| `kafkaKey` | Original Kafka record key (may be null) |
| `originalBytes` | Raw Kafka value bytes — preserved for DLQ use in any operator |
| `headers` | Kafka message headers as `Map<String, byte[]>` |
| `payload` | Business `Row` (null at source; populated by `ValidateSplitFlattenFunction`) |

Immutable; `withPayload(Row)` returns a new instance with all metadata preserved.

#### `com.pipeline.common.DlqRecord`

Carries a failed message to the DLQ: original bytes, error message, error class, timestamp, source topic/partition/offset. Serialized to Kafka JSON via `DlqSerializationSchema`.

#### `com.pipeline.common.PaginationSchema`

Immutable `Serializable` value object holding four field names for the output JSON page:

| Field | Role |
|-------|------|
| `arrayFieldName` | Name of the array in the output (e.g. `"persons"`) |
| `indexFieldName` | 0-based page index (e.g. `"index"`) |
| `totalFieldName` | Total page count (e.g. `"total"`) |
| `countFieldName` | Items in this page (e.g. `"count"`) |

---

### Merged Operator: ValidateSplitFlattenFunction

#### `com.pipeline.ValidateSplitFlattenFunction`

`ProcessFunction<ProcessedMessage, ProcessedMessage>` — the core pipeline operator.

**Processing steps per message (single JSON parse):**

1. Parse `byte[]` → `JsonNode` (once).
2. Validate required top-level fields against input schema → DLQ on failure.
3. Validate required fields in every array item → DLQ on failure.
4. Split the input array into pages of `pageSize` items.
5. For each page: write pagination metadata and flatten array items directly into a named `Row` — no intermediate `byte[]` or `ObjectNode` page is constructed.

DLQ tag: `"dlq-validate-split-flatten"`.

**Conversion count vs. former three-operator chain:**
```
Former:  byte[] → JsonNode → byte[] (page) → JsonNode → Row   (4 conversions)
Now:     byte[] → JsonNode → Row                               (2 conversions)
```

---

### Schema Utilities

#### `com.pipeline.splitting.schema.SchemaAnalyzer`

Pure-static utility providing three independent capabilities:

**1. `loadSchema(String classpathPath) → JsonNode`**

Centralized schema file loader — used by `JsonFlattenPipeline`, `CsvSerializer`, and `CsvDeserializer`. Reads the resource from the classpath and returns a parsed `JsonNode`. Throws `IllegalStateException` if not found.

**2. `analyze(JsonNode) → PaginationSchema`**

Derives a `PaginationSchema` from any JSON Schema document, regardless of how properties are declared. Supports flat `"properties"`, `allOf`/`anyOf`/`oneOf`, `if`/`then`/`else`, and `$defs`/`definitions` with `$ref` resolution.

Detection heuristics:

| Role | Rule |
|------|------|
| Array data field | Property with `"type": "array"` |
| Total pages field | Only integer with `"minimum": 1` |
| Page index field | Integer with `"minimum": 0` + description containing `"index"`, `"current"`, or `"0-based"` |
| Item count field | The sole remaining integer property |

Throws `IllegalArgumentException` if any role cannot be uniquely identified.

**3. `extractCsvColumns(JsonNode) → List<String>`**

Extracts an **ordered** list of CSV column names from a JSON Schema:

- The schema's `"required"` array defines every column and their order — there are no optional CSV columns.
- If a `"properties"` object is present, every key in it must also appear in `"required"` (validation enforced at load time).
- Column names may use dot-notation to reference flat `Row` fields (e.g. `"address.street"`).
- Returns an immutable list.

#### `com.pipeline.validation.InputSchemaInfo`

Immutable value object produced by `InputSchemaInfo.analyze(JsonNode)`. Extracts:

- **Array field name** — first `"type":"array"` property at root.
- **Required fields** — all required fields at any depth as dot-notation paths.
- **Required item fields** — required fields from the array's `"items"` sub-schema.

Handles `allOf` (union), `anyOf`/`oneOf` (intersection) composition keywords.

#### `com.pipeline.splitting.page.PageBuilder`

Pure-static utility: builds one output page `ObjectNode` from a `PaginationSchema`, an `ArrayNode` slice, a page index, and a total count. No Flink dependency.

---

### Deserialization

#### `com.pipeline.deserialization.KafkaEnvelopeDeserializer`

`KafkaRecordDeserializationSchema<ProcessedMessage>` — Kafka source deserializer. Captures full Kafka envelope (key, value bytes, headers) into a `ProcessedMessage` with `payload=null`.

#### `com.pipeline.deserialization.CsvDeserializer`

`ProcessFunction<byte[], Row>` — converts a raw UTF-8 CSV line into a named-field `Row`.

**Column contract:** Column names and order come from `SchemaAnalyzer.loadSchema` + `SchemaAnalyzer.extractCsvColumns`. The schema's `"required"` array defines every column. All fields are mandatory — column count mismatch routes to `DLQ_TAG ("dlq-csv")`.

**Field mapping:** positional — `column[i] → row.setField(column[i], value[i])`. Empty string → `null` in Row.

**RFC 4180 parsing:** quoted fields, `""` escape for embedded quotes, trailing comma = empty final field.

The column list is stored as a plain `List<String>` serialized with the operator — no file I/O at worker start-up.

---

### Serialization

#### `com.pipeline.serialization.ReconstructSerializer`

`KafkaRecordSerializationSchema<ProcessedMessage>` — inverts the flattening step.

Reads a named-field `Row`, reconstructs the original hierarchical JSON by interpreting each dot-notation key as a nested path. Numeric segments become JSON array indices. Uses `TreeMap` for lexicographic ordering (parent paths before children). Propagates original Kafka key and headers onto the output record.

#### `com.pipeline.serialization.CsvSerializer`

`KafkaRecordSerializationSchema<ProcessedMessage>` — serializes a `Row` to a flat CSV line.

**Column contract:** Column names and order come from `SchemaAnalyzer.loadSchema` + `SchemaAnalyzer.extractCsvColumns`. The schema's `"required"` array defines every column. Fields absent from the Row are written as empty strings.

**RFC 4180 encoding:** values containing `,`, `"`, `\n`, or `\r` are wrapped in double-quotes; embedded `"` is doubled to `""`.

The column list is stored as a plain `List<String>` serialized with the operator — no file I/O at worker start-up.

#### `com.pipeline.serialization.DlqSerializationSchema`

`KafkaRecordSerializationSchema<DlqRecord>` — serializes DLQ records to JSON for the dead letter topic.

#### `com.pipeline.common.typeinfo.FlatRowSerializer`

Custom `TypeSerializer<Row>` for named-field `Row` objects. Avoids Kryo.

Wire format per record:
```
[rowKind: byte] [fieldCount: int]
for each field:
  [name: UTF] [tag: byte] [value: typed]
```

Tags: 0=null, 1=String, 2=Integer, 3=Long, 4=Double, 5=Float, 6=Boolean, 7=BigDecimal.

**Important:** all integer JSON numbers survive as `Long` after passing through this serializer.

---

### Processing

#### `com.pipeline.processing.UpperCaseMapFunction`

`MapFunction<ProcessedMessage, ProcessedMessage>` — applies a string processor to selected named fields.

**Field matching** — `matches(String pattern, String fieldName)`:
- Exact match: `"person.name"` matches only `"person.name"`
- Wildcard: `*` as a standalone dot-separated segment matches any single segment
  - `"search_engines.*.imdb.director"` matches `"search_engines.0.imdb.director"`, `"search_engines.4.imdb.director"`, etc.

**Processor backends:**
1. **Vendor JAR** (production): loaded once per TaskManager subtask via `URLClassLoader`; a bound `MethodHandle` resolves the vendor class's `String process(String)` method (~10× faster than `Method.invoke()` after JIT warm-up)
2. **Fallback** (development/testing): `String.toUpperCase(Locale.ROOT)` when `processing.third-party-jar` is blank

---

## Schema Resources

| File | Description |
|------|-------------|
| `schemas/input-person.schema.json` | Single person object; `required: [firstName, lastName, age]`; nested `address` object |
| `schemas/output-persons-paginated.schema.json` | Paginated persons page; `required: [persons, index, total, count]` |
| `schemas/input-netflix-categories.schema.json` | Nested Netflix categories input; `application` object + `search_engines` array with IMDB sub-objects |
| `schemas/output-netflix-flat.schema.json` | Flat Netflix output; all fields at root level |
| `schemas/csv-person-flat.schema.json` | CSV schema for a person record; `required: [firstName, lastName, age, address.street]` — all fields mandatory, order matches CSV columns |

### CSV Schema Contract

CSV schemas use standard JSON Schema format with one rule: **the `"required"` array defines both the complete set of columns and their order**. All properties must be required (no optional CSV columns). Column names may use dot-notation to reference flat `Row` fields.

```json
{
  "required": ["firstName", "lastName", "age", "address.street"],
  "properties": {
    "firstName":      { "type": "string"  },
    "lastName":       { "type": "string"  },
    "age":            { "type": "integer" },
    "address.street": { "type": "string"  }
  }
}
```

`SchemaAnalyzer.extractCsvColumns()` validates this contract at construction time and returns an immutable ordered list.

---

## Checkpointing & Exactly-Once

- Source: `isolation.level=read_committed` (skips uncommitted Kafka transactions)
- Sink: `EXACTLY_ONCE` delivery guarantee via Kafka transactions
- `transaction.timeout.ms` must exceed `checkpoint.interval-ms + checkpoint.timeout-ms`
- Aligned or unaligned checkpoints configurable via `checkpoint.unaligned`

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| `Row.withNames()` instead of POJO | Schema-agnostic; field set determined at runtime from JSON |
| Dot-notation path keys | Simple, consistent, round-trippable; no nested Row objects needed |
| Merge validate + split + flatten into one operator | Eliminates intermediate `byte[]` serialization; single JSON parse per message |
| `ProcessedMessage` envelope | Preserves Kafka key and headers through all operators for DLQ and output propagation |
| `PaginationSchema` value object | Decouples field name knowledge from splitting logic; enables schema-agnostic splitter |
| `SchemaAnalyzer` as shared schema utility | Single place for file loading (`loadSchema`) and all schema analysis — pagination, CSV columns; no duplication |
| CSV column order from `required` array | `required` is already an ordered JSON array; no extra extension needed; same schema format as JSON |
| `ThreadLocal<ObjectMapper>` in Flink operators | ObjectMapper is not serializable; one instance per thread avoids contention |
| `MethodHandle` for vendor processor | ~10× faster than reflection `Method.invoke()` after JIT warm-up; loaded once per subtask |
| Column list serialized with operator | `List<String>` serializes with the operator graph; no file I/O at worker start-up |
| Wildcard `*` per dot-segment | Matches array indices naturally; simple to reason about; no regex overhead |
