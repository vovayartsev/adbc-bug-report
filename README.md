# Bug Report: PostgreSQL Driver Cannot Handle NULL/nil Parameter Values

## Summary

The PostgreSQL ADBC driver fails when attempting to bind `NULL`/`nil`/`None` values as query parameters, throwing an error: `"Can't map Arrow type 'na' to Postgres type"`. This prevents using parameterized queries with null values, which is a common requirement for INSERT operations with incomplete data.

## Environment

- **ADBC Version**: Tested on 0.11.0 and confirmed still present in latest main branch (1.9.0)
- **Driver**: PostgreSQL (`adbc_driver_postgresql`)
- **Database**: PostgreSQL (any version)
- **Languages Affected**: Python, Elixir (likely all language bindings)

## Expected Behavior

Should be able to execute parameterized queries with NULL values:

```python
cursor.execute("INSERT INTO users (name, email) VALUES ($1, $2)", ("Alice", None))
```

```elixir
Adbc.Connection.query(conn, "INSERT INTO users (name, email) VALUES ($1, $2)", ["Alice", nil])
```

The NULL value should be properly bound to the parameter and inserted as NULL in the database.

## Actual Behavior

The query fails with the following error:

```
INTERNAL: nanoarrow call failed: PostgresType::FromSchema(...) = (95) Operation not supported.
Can't map Arrow type 'na' to Postgres type
```

## Root Cause

The issue is in the PostgreSQL driver's type mapping code. The `PostgresType::FromSchema` function in `c/driver/postgresql/postgres_type.h` does not have a case for `NANOARROW_TYPE_NA`:

```cpp
inline ArrowErrorCode PostgresType::FromSchema(const PostgresTypeResolver& resolver,
                                               ArrowSchema* schema, PostgresType* out,
                                               ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kBool), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    // ... other types ...

    default:
      ArrowErrorSet(error, "Can't map Arrow type '%s' to Postgres type",
                    ArrowTypeString(schema_view.type));
      return ENOTSUP;  // <-- NA type hits this default case
  }
}
```

When a NULL/nil value is passed as a parameter, it gets represented as Arrow type `na` (NANOARROW_TYPE_NA), which has no corresponding case in the switch statement.

## Comparison with SQLite Driver

The SQLite ADBC driver handles NULL values correctly in parameterized queries, suggesting this is specific to the PostgreSQL driver implementation.

## Minimal Reproducible Examples

### Python Example (Recommended)

A complete, tested, working reproduction script is provided in `python_example.py`.

**Requirements**: `pip install adbc-driver-postgresql adbc-driver-manager pyarrow`

**Test Results**:
```
✓ Test 1: Non-NULL values inserted successfully
✗ Test 2: NULL value fails with "Can't map Arrow type 'na' to Postgres type"
✗ Test 3: Multiple NULLs fail with the same error
```

### Elixir Example

See `elixir_example.exs` - demonstrates the same issue in the Elixir ADBC wrapper.

### C Example (Reference)

See `c_example.c` - provided as a reference for maintainers (requires building ADBC from source).

## Workaround

Currently, there is no clean workaround. Users must either:
1. Avoid using NULL values in parameterized queries
2. Construct SQL strings dynamically (unsafe, prone to SQL injection)
3. Use typed nullable columns with explicit type hints (complex and not always possible)

## Impact

This issue affects any use case involving:
- Bulk INSERT operations with incomplete data
- UPDATE queries that need to set fields to NULL
- Any parameterized query where NULL is a valid parameter value

## Related Issues

- Similar to: https://github.com/livebook-dev/adbc/issues/136 (Elixir wrapper)
- [Issue 81](https://github.com/apache/arrow-adbc/issues/81) claimed to handle null values, but appears incomplete

## References

- Error occurs in: `c/driver/postgresql/postgres_type.h` (PostgresType::FromSchema)
- Called from: `c/driver/postgresql/statement.cc` (parameter binding code)
