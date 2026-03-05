# Test Scenarios

All tests live under `src/test/java/com/pipeline/`.

Run all tests:
```bash
mvn test
```

Run a single test class:
```bash
mvn test -Dtest="UpperCaseMapFunctionTest"
```

---

## 1. SchemaAnalyzerTest

**Package:** `com.pipeline.splitting`
**File:** `splitting/SchemaAnalyzerTest.java`
**Type:** Pure unit tests — Jackson only, no Flink dependency

| # | Test | Verifies |
|---|------|---------|
| 1 | `analyze_personsPaginatedSchema` | Real classpath resource `output-persons-paginated.schema.json` produces correct `PaginationSchema` (`persons/index/total/count`) |
| 2 | `analyze_schemaWithoutRequired` | Schema with `properties` but no `required` array is fully supported |
| 3 | `analyze_allOf_propertiesSplitAcrossSubSchemas` | All four fields in a single `allOf` branch are collected |
| 4 | `analyze_allOf_eachBranchHasOneProperty` | One property per `allOf` entry (extreme split) still resolves correctly |
| 5 | `analyze_anyOf_propertiesInBranches` | Two `anyOf` branches each contributing two fields |
| 6 | `analyze_oneOf_propertiesInBranches` | Properties inside `oneOf` are collected |
| 7 | `analyze_nestedComposition_allOfInsideAnyOf` | Deeply nested `anyOf → allOf → properties` |
| 8 | `analyze_ifThenElse_propertiesInBranches` | Fields split between `then` and `else` branches |
| 9 | `analyze_defsWithRef_propertiesResolvedFromDefs` | `$defs` with `$ref` resolution via JSON Pointer (`#/$defs/Foo`) |
| 10 | `analyze_definitionsKeyword_resolvedFromDefinitions` | Legacy `definitions` keyword resolved via `#/definitions/Foo` |
| 11 | `analyze_topLevelPropertiesMixedWithAllOf` | Two fields at root `properties` + two in `allOf` |
| 12 | `analyze_customSchemaWithDifferentFieldNames` | `orders/pageIndex/pageCount/itemCount` — proves schema-agnostic analysis |
| 13 | `collectProperties_traversesAllCompositionKeywords` | `p1`–`p7` collected from `properties`, `allOf`, `anyOf`, `oneOf`, `if`, `then`, `else` simultaneously |
| 14 | `collectProperties_doesNotDescendIntoPropertyValues` | Sub-properties of a nested object property are NOT collected (avoids false matches) |
| 15 | `analyze_inputPersonSchema_throwsBecauseNoArrayField` | `input-person.schema.json` (no array property) → `IllegalArgumentException` mentioning "array" |
| 16 | `analyze_throwsIfNoArrayField` | Schema with only an integer property → `IllegalArgumentException` |
| 17 | `analyze_throwsIfNoPropertiesAnywhere` | Completely empty schema → `IllegalArgumentException` |
| 18 | `analyze_throwsIfAmbiguousIndexField` | Two integer fields both matching the index heuristic → `IllegalArgumentException` |

---

## 2. PageBuilderTest

**Package:** `com.pipeline.splitting`
**File:** `splitting/PageBuilderTest.java`
**Type:** Pure unit tests — Jackson only, no Flink dependency

| # | Test | Verifies |
|---|------|---------|
| 1 | `build_setsAllFourFields` | Output `ObjectNode` contains index, total, count, and array fields with correct values |
| 2 | `build_preservesArrayContent` | All three items from the input slice appear in the output array, in order |
| 3 | `build_countReflectsActualItemsSize` | Last page with one item gets `count=1` regardless of page size |
| 4 | `build_emptySliceProducesZeroCount` | Empty slice → `count=0`, empty array |

---

## 3. ArraySplitterFunctionTest

**Package:** `com.pipeline.splitting`
**File:** `splitting/ArraySplitterFunctionTest.java`
**Type:** Flink integration tests — `OneInputStreamOperatorTestHarness` with `ProcessOperator`

| # | Test | Verifies |
|---|------|---------|
| 1 | `singlePage_arrayFitsInOnePage` | 3 persons, pageSize=10 → 1 page; `index=0`, `total=1`, `count=3` |
| 2 | `multiPage_correctPagination` | 5 persons, pageSize=2 → 3 pages with index 0/1/2 and counts 2/2/1 |
| 3 | `lastPage_hasCorrectCount` | Last page (5 items, pageSize=2) has `count=1` and array size 1 |
| 4 | `allOutputsMatchOutputSchema` | Every page carries all four schema fields (`persons`, `index`, `total`, `count`) |
| 5 | `emptyBytes_routedToDlq` | Empty `byte[]` → DLQ side output; no main output |
| 6 | `missingArrayField_routedToDlq` | JSON without `"persons"` field → DLQ; error message names the missing field |
| 7 | `differentSchema_personsThenOrders` | Swapping to `orders/pageIndex/pageCount/itemCount` schema — output uses new field names, old names absent |

---

## 4. MessageValidatorFunctionTest

**Package:** `com.pipeline.validation`
**File:** `validation/MessageValidatorFunctionTest.java`
**Type:** Flink integration tests + pure unit tests of `validate()`

### Scenario 1 — Missing top-level required field

| # | Test | Input | Expected |
|---|------|-------|---------|
| 1 | `missingTopLevelRequiredField_routedToDlq` | `{"data":[…]}` (no `"persons"` key) | 1 DLQ record; error mentions `"persons"` |
| 2 | `missingTopLevelRequiredField_errorMessageContainsFieldName` | `{"items":[]}` with validator requiring `items` + `requestId` | DLQ error mentions `"requestId"` |

### Scenario 2 — Required field missing inside array item

| # | Test | Input | Expected |
|---|------|-------|---------|
| 3 | `arrayItemMissingRequiredField_routedToDlq` | `{"persons":[{"firstName":"John","age":30}]}` (no `lastName`) | DLQ; error mentions `"lastName"` and `"persons[0]"` |
| 4 | `arrayItemMissingMultipleFields_allViolationsReportedInDlq` | Person with only `firstName` (missing `lastName` + `age`) | DLQ error mentions both `"lastName"` and `"age"` |
| 5 | `lastArrayItemMissingField_correctIndexReportedInDlq` | 3 persons; third missing `age` | DLQ error mentions `"age"` and `"persons[2]"` |
| 6 | `multipleItemsMissingFields_allViolationsCollectedInSingleDlqRecord` | 3 persons all missing `age` | Single DLQ record mentioning `persons[0]`, `persons[1]`, `persons[2]` |

### Valid messages — pass-through

| # | Test | Verifies |
|---|------|---------|
| 7 | `validMessage_allFieldsPresent_passesThroughUnchanged` | 1 main output; 0 DLQ; output bytes identical to input bytes |
| 8 | `validMessage_multiplePersonsAllPresent_passesThrough` | 2 valid persons → 1 main output, 0 DLQ |

### Edge cases

| # | Test | Verifies |
|---|------|---------|
| 9 | `emptyBytesInput_routedToDlq` | Zero-length input → DLQ |
| 10 | `noItemRequirements_topLevelOnlyValidator_passesIncompleteItems` | Item-level validation disabled → arbitrary items pass through |
| 11 | `emptyArray_noItemsToValidate_passesThrough` | `{"persons":[]}` → passes (nothing to validate at item level) |

### Unit tests of `validate()` (no Flink harness)

| # | Test | Verifies |
|---|------|---------|
| 12 | `validate_missingTopLevelField_returnsViolation` | Missing `tenantId` → 1 violation containing `"tenantId"` |
| 13 | `validate_missingItemField_returnsViolationWithIndex` | Missing `name` in `persons[0]` → violation with `"name"` and `"[0]"` |
| 14 | `validate_allPresent_returnsEmptyList` | Valid input → empty violations list |

---

## 5. FlatteningDeserializerTest

**Package:** `com.pipeline.serialization`
**File:** `serialization/FlatteningDeserializerTest.java`
**Type:** Flink integration tests — `OneInputStreamOperatorTestHarness`

Tests verify:
- Nested JSON objects flatten to `parent.child` dot-notation keys
- JSON arrays flatten to `array.0`, `array.1`, … keys
- Complex deeply-nested structures (persons with addresses, arrays within objects)
- Null handling modes (`INCLUDE`, `EXCLUDE`, `REPLACE_EMPTY_STRING`)
- Invalid JSON routes to `DLQ_TAG`

---

## 6. FlatRowSerializerTest

**Package:** `com.pipeline.serialization`
**File:** `serialization/FlatRowSerializerTest.java`
**Type:** Pure unit tests

Tests verify round-trip serialization/deserialization of `Row` objects through `FlatRowSerializer`:
- All supported value types: `String`, `Integer`, `Long`, `Double`, `Float`, `Boolean`, `BigDecimal`, `null`
- Multiple fields per row
- Field names with dot notation

**Note:** `Integer` values written to a `Row` round-trip as `Long` through `FlatRowSerializer` (wire format uses tag 2=Integer vs 3=Long, but Jackson's JSON integer parsing promotes to Long).

---

## 7. ReconstructSerializerTest

**Package:** `com.pipeline.serialization`
**File:** `serialization/ReconstructSerializerTest.java`
**Type:** Pure unit tests

Tests verify `Row → JSON bytes` reconstruction:
- Flat fields reconstruct to flat JSON
- Dot-notation keys reconstruct nested objects
- Numeric dot segments reconstruct JSON arrays
- Mixed nested + array paths

---

## 8. NetflixCategoriesTest

**Package:** `com.pipeline`
**File:** `NetflixCategoriesTest.java`
**Type:** End-to-end integration tests — full Netflix JSON fixture (5 search engines)

### Scenario A — No splitting: input schema == output schema

The full Netflix JSON is flattened directly into a single `Row` without array splitting and reconstructed back. Output JSON must be structurally equal to input.

| # | Test | Verifies |
|---|------|---------|
| 1 | `scenarioA_noSplitting_singleInputProducesSingleFlatRow` | `FlatteningDeserializer` produces exactly 1 Row; spot-checks application fields, engine[0] and engine[4], and IMDB fields |
| 2 | `scenarioA_roundTrip_reconstructedJsonEqualsOriginalInput` | `ReconstructSerializer` output is structurally equal (Jackson `JsonNode.equals`) to the original Netflix JSON |
| 3 | `scenarioA_flatRowFieldCount_matchesExpectedTotalLeaves` | Flat Row has exactly 73 fields: 3 app fields + 5 engines × 14 leaf fields |

### Scenario B — Split on `search_engines`: one input → multiple output messages

`ArraySplitterFunction` with `pageSize=2` on the 5-engine array → 3 pages (counts: 2, 2, 1).
Uses `PaginationSchema("search_engines", "page_index", "total_pages", "page_count")` — field names deliberately differ from the persons schema to prove schema-agnosticism.

| # | Test | Verifies |
|---|------|---------|
| 4 | `scenarioB_splitProducesThreePagesFromFiveEngines` | `⌈5/2⌉ = 3` pages produced |
| 5 | `scenarioB_paginationMetadataIsCorrectOnAllPages` | Each page: correct `page_index` (0/1/2), `total_pages=3`, `page_count` (2/2/1), `search_engines` is an array of expected size |
| 6 | `scenarioB_firstPageContainsCorrectSearchEngines` | Page 0 has "Action & Adventure" and "Action Comedies" |
| 7 | `scenarioB_lastPageHasOneEngineWithCorrectData` | Page 2 has 1 engine: "Anime Series", `last_used=12`, `imdb_rating=8.3` |
| 8 | `scenarioB_eachPageCanBeFlattenedIndependently` | Each page flattens into exactly 1 Row with correct `page_index` (as `Long`) and `total_pages=3L` |
| 9 | `scenarioB_allFiveEnginesReconstructedAcrossPages` | Collecting names across all pages produces all 5 engines in original order |

---

## 9. UpperCaseMapFunctionTest

**Package:** `com.pipeline.processing`
**File:** `processing/UpperCaseMapFunctionTest.java`
**Type:** Unit tests for `matches()` + Flink integration tests — `OneInputStreamOperatorTestHarness` with `StreamMap`

All integration tests use `FlatteningDeserializer` to produce real flattened `Row` objects from inline JSON, then apply `UpperCaseMapFunction` in fallback mode (no vendor JAR).

### Unit tests — `matches(String pattern, String fieldName)` (static, package-private)

| # | Test | Input | Expected |
|---|------|-------|---------|
| 1 | `matches_exactPattern_sameField_returnsTrue` | `"person.name"` vs `"person.name"` | `true` |
| 2 | `matches_exactPattern_differentField_returnsFalse` | `"person.name"` vs `"person.age"` | `false` |
| 3 | `matches_wildcardMiddleSegment_numericIndex_returnsTrue` | `"search_engines.*.imdb.director"` vs `"search_engines.0.imdb.director"` and `"…4…"` | `true` both |
| 4 | `matches_wildcardMiddleSegment_wrongTrailingField_returnsFalse` | `"search_engines.*.imdb.director"` vs `"search_engines.0.imdb.title"` | `false` |
| 5 | `matches_wildcardPattern_segmentCountMismatch_returnsFalse` | 4-segment pattern vs 2-segment field | `false` |
| 6 | `matches_noWildcard_differentIndex_returnsFalse` | `"search_engines.0.imdb.director"` vs `"search_engines.1.imdb.director"` | `false` |
| 7 | `matches_wildcardAloneAsEntirePattern_matchesAnyTopLevelKey` | `"*"` vs `"name"` → true; `"*"` vs `"person.name"` → false (segment mismatch) | As expected |

### Netflix — specific director (exact pattern)

Fixture: 3-engine Netflix JSON. Pattern: `"search_engines.0.imdb.director"`.

| # | Test | Verifies |
|---|------|---------|
| 8 | `netflix_exactPattern_uppercasesOnlyTargetDirector` | Engine 0 director → `"SAM HARGRAVE"`; engines 1 and 2 directors unchanged |
| 9 | `netflix_exactPattern_nonDirectorFieldsUnchanged` | `imdb.title`, `application.name`, `search_engines.0.name` all unchanged |

### Netflix — all directors (wildcard pattern)

Pattern: `"search_engines.*.imdb.director"`.

| # | Test | Verifies |
|---|------|---------|
| 10 | `netflix_wildcardPattern_uppercasesAllDirectors` | All three directors uppercased: `"SAM HARGRAVE"`, `"DAVID LEITCH"`, `"GEORGE MILLER"` |
| 11 | `netflix_wildcardPattern_nonDirectorFieldsUntouched` | `imdb.genre` fields and `application.name` unchanged |

### Persons — firstName uppercase

Fixture: 2-person persons JSON.

| # | Test | Pattern | Verifies |
|---|------|---------|---------|
| 12 | `person_exactPattern_uppercasesOnlyFirstPersonName` | `"persons.0.firstName"` | `persons.0.firstName="JOHN"`; lastName and `persons.1.firstName` unchanged |
| 13 | `person_wildcardPattern_uppercasesAllFirstNames` | `"persons.*.firstName"` | Both `"JOHN"` and `"JANE"`; both lastNames unchanged |

### Multiple patterns in a single pass

| # | Test | Verifies |
|---|------|---------|
| 14 | `multiplePatterns_allMatchingFieldsUppercased` | `["search_engines.0.imdb.director", "application.name"]` → both uppercased; engine 1 director unchanged |
| 15 | `multiplePatterns_wildcardActorsAndExactTitle` | `["search_engines.*.imdb.actors", "search_engines.1.imdb.title"]` → all 3 actors uppercased; only title[1] uppercased; titles[0] and [2] unchanged |

---

## Test Infrastructure Notes

### Flink Test Harness setup patterns

**For `ProcessFunction<byte[], Row>` (e.g. `FlatteningDeserializer`):**
```java
var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
harness.setup(FlatRowSerializer.INSTANCE);
harness.open();
harness.processElement(bytes, System.currentTimeMillis());
List<Row> rows = harness.extractOutputValues();
harness.close();
```

**For `ProcessFunction<byte[], byte[]>` (e.g. `ArraySplitterFunction`, `MessageValidatorFunction`):**
```java
var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
harness.setup(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
        .createSerializer(new ExecutionConfig()));
harness.open();
harness.processElement(bytes, System.currentTimeMillis());
List<byte[]> out = harness.extractOutputValues();
harness.close();
```

**For `RichMapFunction<Row, Row>` (e.g. `UpperCaseMapFunction`):**
```java
var harness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(fn));
harness.setup(FlatRowSerializer.INSTANCE);
harness.open();
harness.processElement(row, System.currentTimeMillis());
List<Row> out = harness.extractOutputValues();
harness.close();
```

### Reading DLQ side output
`getSideOutput()` returns `ConcurrentLinkedQueue<StreamRecord<X>>` in Flink 1.20 — must unwrap:
```java
Queue<?> rawDlq = harness.getSideOutput(MyFunction.DLQ_TAG);
List<DlqRecord> dlq = new ArrayList<>();
if (rawDlq != null) {
    rawDlq.forEach(sr ->
        dlq.add((DlqRecord) ((StreamRecord<?>) sr).getValue()));
}
```

### Integer → Long promotion
All integer-valued fields in a `Row` round-tripped through `FlatRowSerializer` come back as `Long`.
Use `Long` literals in assertions: `assertEquals(3L, row.getField("total_pages"))`.
