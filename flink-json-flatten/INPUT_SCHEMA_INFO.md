# InputSchemaInfo

## Overview

`InputSchemaInfo` is a compact runtime summary of an input JSON Schema.

Instead of walking the raw schema every time a message is validated, the code first analyzes the schema and stores the important parts in an immutable Java object. That object is then reused by the JSON input path, mainly in:

- `InputSchemaInfo.analyze(...)`
- `JsonInputProcessFunction`
- `JsonRowFlattener.validate(...)`

In simple terms, `InputSchemaInfo` answers these questions:

- What is the top-level array field?
- Which fields are always required?
- Which fields are required only in `oneOf` / `anyOf` branches?
- What type is each known field supposed to have?

---

## Main Fields in InputSchemaInfo

### `arrayFieldName`

```java
private final String arrayFieldName;
```

The name of the top-level array property in the schema.

Example:

```json
"persons": {
  "type": "array",
  "items": { "$ref": "#/$defs/person" }
}
```

Result:

```text
persons
```

This is used to locate item-level schema rules and item-level validation metadata.

---

### `requiredFields`

```java
private final List<String> requiredFields;
```

A flat list of **root/object fields that are always required**.

These are stored as dot-paths.

Examples:

- `metadata`
- `persons`
- `metadata.totalCount`
- `metadata.page`
- `metadata.apiVersion`

Important semantic rule:

- For `allOf`: required fields are merged in.
- For `oneOf` / `anyOf`: only the fields required in **every** branch are kept here.

So this list means:

> fields that must exist in every valid payload

This list is used directly by runtime validation.

---

### `requiredItemFields`

```java
private final List<String> requiredItemFields;
```

A flat list of **array item fields that are always required**.

These paths are relative to each array item, not to the whole document.

Examples:

- `firstName`
- `lastName`
- `address`
- `address.street`
- `address.city`

If the top-level array is `persons`, validation turns item paths like `firstName` into real message paths such as:

- `persons.0.firstName`
- `persons.1.address.street`

Important semantic rule:

- `requiredItemFields` only contains fields required in all valid item shapes.
- It does **not** include branch-specific required fields from `oneOf` / `anyOf` unless every branch requires them.

---

### `conditionalRequiredFields`

```java
private final List<String> conditionalRequiredFields;
```

A flat list of **root/object fields that are required in one or more alternative branches**, but not necessarily in all branches.

This is **schema introspection metadata**, not unconditional validation metadata.

Use this when you want to answer:

> Which fields are required by at least one valid alternative shape?

---

### `conditionalRequiredItemFields`

```java
private final List<String> conditionalRequiredItemFields;
```

A flat list of **item-level fields required by one or more `oneOf` / `anyOf` branches**.

This is the important field for schemas like `complex.json`.

For example, if `grades.items` contains three branches:

- one branch requires `school`
- one branch requires `highschool`
- one branch requires `mathGarde` and `historyGrade`

then those paths appear here:

- `grades.school`
- `grades.highschool`
- `grades.mathGarde`
- `grades.historyGrade`

These are exposed for schema visibility and debugging, but they are **not** treated as unconditional required fields during runtime validation.

That avoids false failures where a valid `school` branch would be rejected just because `highschool` is absent.

---

### `fieldTypes`

```java
private final Map<String, String> fieldTypes;
```

A map of root/object dot-paths to their expected schema type.

Examples:

- `metadata.totalCount -> integer`
- `metadata.timestamp -> string`
- `persons -> array`

This is used for type validation when a field exists in the payload.

---

### `itemFieldTypes`

```java
private final Map<String, String> itemFieldTypes;
```

A map of item-relative dot-paths to their expected schema type.

Examples:

- `firstName -> string`
- `age -> integer`
- `address.street -> string`
- `grades -> array`
- `grades.mathGarde -> integer`
- `grades.school.historyGrade -> integer`

This is also used by runtime validation.

---

## Constructor Meaning

The constructor simply freezes the analyzed schema summary into one immutable value object:

```java
public InputSchemaInfo(
        String arrayFieldName,
        List<String> requiredFields,
        List<String> requiredItemFields,
        List<String> conditionalRequiredFields,
        List<String> conditionalRequiredItemFields,
        Map<String, String> fieldTypes,
        Map<String, String> itemFieldTypes)
```

You can think of the object as four groups of metadata:

- array shape
- unconditional required fields
- conditional/branch required fields
- type information

---

## How `analyze(...)` Builds It

```java
public static InputSchemaInfo analyze(JsonNode schema)
```

This method analyzes the raw schema and builds the object in roughly this order:

1. Find the root array field.
2. Extract unconditional required root fields.
3. Extract unconditional required item fields.
4. Extract conditional required root fields from `oneOf` / `anyOf` branches.
5. Extract conditional required item fields.
6. Extract root field types.
7. Extract item field types.

---

## Meaning of the Private Helpers

### `findArrayField(...)`

Finds the first root property whose schema type is `array`.

---

### `extractRequired(...)`

Builds the unconditional required field list.

Internally it uses `collectRequiredPaths(...)`.

---

### `collectRequiredPaths(...)`

Recursively walks the schema and collects fields that are required in all valid shapes.

Behavior:

- follows local `$ref`
- descends into nested `properties`
- descends into `items`
- merges `allOf`
- intersects `oneOf` / `anyOf`

This is why `requiredFields` and `requiredItemFields` represent **always-required** fields only.

---

### `extractConditionalRequired(...)`

Builds the conditional required field list.

Internally it uses `collectConditionalRequiredPaths(...)`.

---

### `collectConditionalRequiredPaths(...)`

Recursively walks the schema and collects fields required by alternative branches.

Behavior:

- follows local `$ref`
- descends into nested `properties`
- descends into `items`
- walks `allOf`
- unions branch-required fields from `oneOf` / `anyOf`

This is why branch-specific paths remain visible for introspection.

---

### `intersectRequired(...)`

Used by unconditional required extraction.

For each `oneOf` / `anyOf`, it keeps only the fields present in **every** branch.

That prevents alternative shapes from being treated as if all branches had to be satisfied at once.

---

### `extractLeafTypes(...)`

Builds a flat map:

```text
path -> type
```

Examples:

- `age -> integer`
- `address.street -> string`
- `grades -> array`

---

### `collectLeafTypes(...)`

Recursively walks the schema to discover field types.

Behavior:

- follows local `$ref`
- descends into `properties`
- descends into `items`
- walks `allOf`, `oneOf`, and `anyOf`
- preserves container types such as `array` and `object`

This is why a path like `grades` can be recorded as `array` while nested fields such as `grades.school.mathGarde` are also visible.

---

### `shouldTraverseSchema(...)`

Decides whether a schema node should be explored recursively.

It returns true for nodes that are or contain:

- `object`
- `array`
- `properties`
- `items`
- `$ref`
- `allOf`
- `anyOf`
- `oneOf`

---

### `resolveLocalRef(...)`

Resolves local references such as:

```json
"$ref": "#/$defs/person"
```

without requiring a separate schema engine.

This is important because many of the repo schemas use `$defs` heavily.

---

## Runtime Meaning

### Used directly for validation

`JsonRowFlattener.validate(...)` uses:

- `getRequiredFields()`
- `getRequiredItemFields()`
- `getFieldTypes()`
- `getItemFieldTypes()`

These drive actual message validation.

### Used for introspection / debugging

The conditional required getters are useful when understanding complex schemas, especially those using `oneOf` or `anyOf`:

- `getConditionalRequiredFields()`
- `getConditionalRequiredItemFields()`

These are not currently treated as unconditional runtime requirements.

---

## Example: `complex.json`

For `complex.json`, the top-level array field is:

```text
persons
```

### Always-required item fields

Examples that appear in `requiredItemFields`:

- `firstName`
- `lastName`
- `address`
- `address.street`

### Branch-required item fields

Examples that appear in `conditionalRequiredItemFields`:

- `grades.school`
- `grades.highschool`
- `grades.mathGarde`
- `grades.historyGrade`

These do not appear in `requiredItemFields` because they belong to different `oneOf` branches.

### Type metadata examples

Examples that appear in `itemFieldTypes`:

- `grades -> array`
- `grades.mathGarde -> integer`
- `grades.historyGrade -> integer`
- `grades.school.mathGarde -> integer`
- `grades.school.historyGrade -> integer`
- `grades.highschool.mathGarde -> integer`
- `grades.highschool.historyGrade -> integer`

---

## Why the Required Fields Are Split in Two

This split is intentional and important.

### `requiredItemFields`
Means:

> required in every valid item

### `conditionalRequiredItemFields`
Means:

> required in one or more valid branches

If these were merged together, validation would become incorrect.

Example:

- a payload that correctly uses the `school` branch would fail just because `highschool` is missing
- a payload that correctly uses direct `mathGarde/historyGrade` would fail just because `school` is missing

So the split preserves correct validation semantics while still making the schema details visible.

---

## Short Summary

- `arrayFieldName` — top-level array name
- `requiredFields` — always-required root fields
- `requiredItemFields` — always-required array item fields
- `conditionalRequiredFields` — root fields required by alternative branches
- `conditionalRequiredItemFields` — item fields required by alternative branches
- `fieldTypes` — expected types for root fields
- `itemFieldTypes` — expected types for item fields

`InputSchemaInfo` is therefore both:

- a validation helper
- and a compact schema introspection model

---

## Related Files

- `src/main/java/com/pipeline/validation/InputSchemaInfo.java`
- `src/main/java/com/pipeline/JsonRowFlattener.java`
- `src/main/java/com/pipeline/JsonInputProcessFunction.java`
- `src/test/java/com/pipeline/JsonRowFlattenerTest.java`
- `src/test/resources/schemas/complex.json`

