# Flink JSON Flatten Pipeline — Design & Implementation

## Overview

A production-ready Apache Flink 1.20 streaming pipeline that:

1. Consumes raw JSON messages from Kafka
2. Splits large arrays into paginated batches
3. Validates required fields before processing
4. Flattens nested JSON into a named-field `Row`
5. Applies string transformations (e.g. uppercase) to selected fields via configurable patterns
6. Reconstructs the `Row` back into hierarchical JSON
7. Publishes results to an output Kafka topic, routing failures to a Dead Letter Queue (DLQ)

---

## Pipeline Topology

```
KafkaSource (input-topic, byte[])
      │
      ▼
MessageValidatorFunction          ──side-output──► DLQ KafkaSink
(ProcessFunction<byte[], byte[]>)
Required-field guard
      │
      ▼
ArraySplitterFunction             ──side-output──► DLQ KafkaSink
(ProcessFunction<byte[], byte[]>)
1 message → N paginated pages
      │ byte[] (one page per emit)
      ▼
FlatteningDeserializer            ──side-output──► DLQ KafkaSink
(ProcessFunction<byte[], Row>)
JSON → Row.withNames() (dot-notation keys)
      │ Row (named)
      ▼
UpperCaseMapFunction
(RichMapFunction<Row, Row>)
Applies string processor to matching fields
      │ Row (mutated)
      ▼
ReconstructSerializer
Row → hierarchical JSON bytes
      │
      ▼
KafkaSink (output-topic, EXACTLY_ONCE)
```

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
| `splitting.input-array-field` | `persons` | Array field name in the raw input |
| `splitting.page-size` | `100` | Max items per output page |
| `flatten.null-handling` | `INCLUDE` | `INCLUDE` / `EXCLUDE` / `REPLACE_EMPTY_STRING` |

---

### Validation

#### `com.pipeline.validation.MessageValidatorFunction`

`ProcessFunction<byte[], byte[]>` — validates required fields before splitting.

**Two validation layers:**

1. **Top-level** — every name in `requiredTopLevelFields` must appear as a direct child of the root JSON object.
2. **Array-item** — every name in `requiredItemFields` must appear in each element of the array identified by `inputArrayField`.

Any violation routes the raw bytes to `DLQ_TAG ("dlq-validator")` with a human-readable error that names the missing field and (for array items) its 0-based index: `"missing field 'lastName' in persons[2]"`.

Valid messages pass through **unchanged** (same bytes, same reference).

---

### Splitting

#### `com.pipeline.splitting.model.PaginationSchema`

Immutable `Serializable` value object holding four field names for the output JSON page:

| Field | Role |
|-------|------|
| `arrayFieldName` | Name of the array in the output (e.g. `"persons"`) |
| `indexFieldName` | 0-based page index (e.g. `"index"`) |
| `totalFieldName` | Total page count (e.g. `"total"`) |
| `countFieldName` | Items in this page (e.g. `"count"`) |

#### `com.pipeline.splitting.schema.SchemaAnalyzer`

Pure-static utility: `JsonNode (schema root) → PaginationSchema`.

**Supports any valid JSON Schema structure** — no mandatory `properties` or `required` at root:

- Flat `"properties"` at root
- Properties inside `allOf` / `anyOf` / `oneOf` sub-schemas (any depth)
- Properties inside `if` / `then` / `else` branches
- Properties in `$defs` / `definitions` referenced by local `$ref` (JSON Pointer resolved)
- Any combination of the above

**Detection heuristics** (work for any conforming schema):

| Role | Rule |
|------|------|
| Array data field | Property with `"type": "array"` |
| Total pages field | Only integer property with `"minimum": 1` |
| Page index field | Integer with `"minimum": 0` + description containing `"index"`, `"current"`, or `"0-based"` (case-insensitive) |
| Item count field | The sole remaining integer property |

Throws `IllegalArgumentException` if any role cannot be uniquely identified.

#### `com.pipeline.splitting.page.PageBuilder`

Pure-static utility: builds one output page `ObjectNode` from a `PaginationSchema`, an `ArrayNode` slice, a page index, and a total count. No Flink dependency.

#### `com.pipeline.splitting.ArraySplitterFunction`

`ProcessFunction<byte[], byte[]>` implementing `ResultTypeQueryable<byte[]>`.

**Algorithm:**
1. Guard: null/empty bytes → `DLQ_TAG ("dlq-splitter")`
2. Parse root JSON
3. Find `inputArrayField`; if missing or not an array → DLQ
4. `totalPages = ⌈arraySize / pageSize⌉`
5. For each page `p`: slice `[p·S, min((p+1)·S, N))`, call `PageBuilder.build()`, emit as `byte[]`
6. Any exception → DLQ

Uses a `ThreadLocal<ObjectMapper>` (one mapper per TaskManager thread, never serialized).

---

### Serialization

#### `com.pipeline.serialization.FlatteningDeserializer`

`ProcessFunction<byte[], Row>` — the core flattening step.

Converts a JSON bytes payload into a Flink `Row` in named-field mode (`Row.withNames()`).
Field names use **dot-notation paths**: nested object keys become `parent.child`, array elements become `array.0`, `array.1`, etc.

Example: `{"a": {"b": [1, 2]}}` → fields `a.b.0 = 1`, `a.b.1 = 2`.

Null handling is configurable via `PipelineConfig.NullHandling`.

Failed parses route to `DLQ_TAG ("dlq-flatten")`.

#### `com.pipeline.serialization.ReconstructSerializer`

`KafkaRecordSerializationSchema<Row>` — inverts the flattening step.

Reads a named-field `Row` and reconstructs the original hierarchical JSON by interpreting each dot-notation key as a nested path. Numeric segments become JSON array indices.

#### `com.pipeline.serialization.typeinfo.FlatRowSerializer`

Custom `TypeSerializer<Row>` for named-field `Row` objects.

Wire format per record:
```
[rowKind: byte] [fieldCount: int]
for each field:
  [name: UTF] [tag: byte] [value: typed]
```

Tags: 0=null, 1=String, 2=Integer, 3=Long, 4=Double, 5=Float, 6=Boolean, 7=BigDecimal.

**Important:** all integer JSON numbers survive as `Long` after passing through this serializer (no `Integer` values in deserialized rows).

---

### Processing

#### `com.pipeline.processing.UpperCaseMapFunction`

`RichMapFunction<Row, Row>` — applies a string processor to selected named fields.

**Field matching** — `matches(String pattern, String fieldName)`:
- Exact match: `"person.name"` matches only `"person.name"`
- Wildcard: `*` as a standalone dot-separated segment matches any single segment (including numeric array indices)
  - `"search_engines.*.imdb.director"` matches `"search_engines.0.imdb.director"`, `"search_engines.4.imdb.director"`, etc.
  - Multiple `*` segments are supported

**Processing behaviour:**
- All fields whose name matches **any** configured pattern are processed — not just the first match
- For each field name, the first pattern that matches wins (subsequent patterns skipped for that field)
- Only `String`-valued fields are processed; other types are skipped silently

**Processor backends:**
1. **Vendor JAR** (production): loaded once per TaskManager subtask via `URLClassLoader`; a bound `MethodHandle` is resolved from the vendor class's `String process(String)` method (~10× faster than `Method.invoke()` after JIT warm-up)
2. **Fallback** (development/testing): `String.toUpperCase(Locale.ROOT)` when `processing.third-party-jar` is blank

Metrics exposed: `row.field.processed` (counter), `row.field.no_match` (counter).

---

## Schema Resources

| File | Description |
|------|-------------|
| `schemas/input-person.schema.json` | Single person object; `required: [firstName, lastName, age]`; no array |
| `schemas/output-persons-paginated.schema.json` | Paginated persons page; `required: [persons, index, total, count]` |
| `schemas/input-netflix-categories.schema.json` | Nested Netflix categories input; `application` object + `search_engines` array with IMDB sub-objects |
| `schemas/output-netflix-flat.schema.json` | Flat Netflix output; all fields at root level (`app_name`, `imdb_*`, etc.) |

---

## Model

#### `com.pipeline.model.DlqRecord`

Carries a failed message to the DLQ: original bytes + error message string.
Serialized to Kafka as JSON via `DlqSerializationSchema`.

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
| `Row.withNames()` instead of `POJO` | Schema-agnostic; field set determined at runtime from JSON |
| Dot-notation path keys | Simple, consistent, round-trippable; no nested Row objects needed |
| `PaginationSchema` value object | Decouples field name knowledge from splitting logic; enables schema-agnostic splitter |
| `SchemaAnalyzer` pure-static | Fully testable without Flink; reusable in non-streaming contexts |
| `ThreadLocal<ObjectMapper>` in Flink operators | ObjectMapper is not serializable; one instance per thread avoids contention |
| `MethodHandle` for vendor processor | ~10× faster than reflection `Method.invoke()` after JIT warm-up; loaded once per subtask |
| Wildcard `*` per dot-segment | Matches array indices naturally; simple to reason about; no regex overhead |
| All-matching field processing | Enables bulk operations (uppercase all directors) with a single pattern |
