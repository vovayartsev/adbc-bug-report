/*
 * Minimal reproducible example for NULL parameter binding issue in PostgreSQL ADBC driver.
 *
 * NOTE: This is a reference example for ADBC maintainers.
 *       Use python_example.py for a working reproduction.
 *
 * Compile (requires ADBC development environment):
 *   gcc -o c_example c_example.c \
 *     -I/path/to/adbc/include \
 *     -I/path/to/nanoarrow/include \
 *     -L/path/to/adbc/lib \
 *     -ladbc_driver_postgresql -ladbc_driver_manager \
 *     -Wl,-rpath,/path/to/adbc/lib
 *
 * Usage:
 *   ./c_example
 *
 * Expected: Should bind NULL values to parameters successfully
 * Actual: Fails with "Can't map Arrow type 'na' to Postgres type"
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "arrow-adbc/adbc.h"
#include "nanoarrow.h"

#define CHECK_ADBC(expr, error) \
  do { \
    AdbcStatusCode code = (expr); \
    if (code != ADBC_STATUS_OK) { \
      fprintf(stderr, "Error: %s\n", (error).message); \
      return 1; \
    } \
  } while (0)

int main() {
  struct AdbcError error = {0};
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;

  // Initialize database
  printf("Initializing PostgreSQL driver...\n");
  CHECK_ADBC(AdbcDatabaseNew(&database, &error), error);
  CHECK_ADBC(AdbcDatabaseSetOption(&database, "driver", "adbc_driver_postgresql", &error), error);
  CHECK_ADBC(AdbcDatabaseSetOption(&database, "uri", "postgresql://user:password@localhost:5432/dbname", &error), error);
  CHECK_ADBC(AdbcDatabaseInit(&database, &error), error);

  // Initialize connection
  CHECK_ADBC(AdbcConnectionNew(&connection, &error), error);
  CHECK_ADBC(AdbcConnectionInit(&connection, &database, &error), error);

  // Create test table
  printf("Creating test table...\n");
  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error), error);
  CHECK_ADBC(AdbcStatementSetSqlQuery(&statement,
    "DROP TABLE IF EXISTS test_nulls",
    &error), error);
  CHECK_ADBC(AdbcStatementExecuteQuery(&statement, NULL, NULL, &error), error);
  AdbcStatementRelease(&statement, &error);

  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error), error);
  CHECK_ADBC(AdbcStatementSetSqlQuery(&statement,
    "CREATE TABLE test_nulls (id SERIAL PRIMARY KEY, name TEXT, email TEXT)",
    &error), error);
  CHECK_ADBC(AdbcStatementExecuteQuery(&statement, NULL, NULL, &error), error);
  AdbcStatementRelease(&statement, &error);

  // Test 1: Insert with non-NULL values (this works)
  printf("\nTest 1: Insert with non-NULL values...\n");
  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error), error);
  CHECK_ADBC(AdbcStatementSetSqlQuery(&statement,
    "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
    &error), error);

  // Create Arrow arrays for parameters
  struct ArrowSchema schema;
  struct ArrowArray array;
  ArrowSchemaInit(&schema);
  ArrowSchemaSetTypeStruct(&schema, 2);
  ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_STRING);
  ArrowSchemaSetName(schema.children[0], "0");
  ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING);
  ArrowSchemaSetName(schema.children[1], "1");

  ArrowArrayInitFromSchema(&array, &schema, NULL);
  ArrowArrayStartAppending(&array);

  // Append values
  ArrowArrayAppendString(array.children[0], ArrowCharView("Alice"));
  ArrowArrayAppendString(array.children[1], ArrowCharView("alice@example.com"));
  ArrowArrayFinishBuildingDefault(&array, NULL);

  CHECK_ADBC(AdbcStatementBind(&statement, &array, &schema, &error), error);
  CHECK_ADBC(AdbcStatementExecuteQuery(&statement, NULL, NULL, &error), error);
  printf("✓ Success: Non-NULL values inserted\n");

  array.release(&array);
  schema.release(&schema);
  AdbcStatementRelease(&statement, &error);

  // Test 2: Insert with NULL value (this fails)
  printf("\nTest 2: Insert with NULL value...\n");
  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error), error);
  CHECK_ADBC(AdbcStatementSetSqlQuery(&statement,
    "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
    &error), error);

  // Create Arrow arrays with NULL value
  ArrowSchemaInit(&schema);
  ArrowSchemaSetTypeStruct(&schema, 2);
  ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_STRING);
  ArrowSchemaSetName(schema.children[0], "0");

  // THIS IS WHERE THE ISSUE OCCURS:
  // Using NANOARROW_TYPE_NA for the second parameter
  ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_NA);
  ArrowSchemaSetName(schema.children[1], "1");

  ArrowArrayInitFromSchema(&array, &schema, NULL);
  ArrowArrayStartAppending(&array);
  ArrowArrayAppendString(array.children[0], ArrowCharView("Bob"));
  ArrowArrayAppendNull(array.children[1], 1);  // Append NULL
  ArrowArrayFinishBuildingDefault(&array, NULL);

  AdbcStatusCode code = AdbcStatementBind(&statement, &array, &schema, &error);
  if (code != ADBC_STATUS_OK) {
    printf("✗ Failed: %s\n", error.message);
    printf("   This is the bug - can't map Arrow type 'na' to Postgres type\n");
  } else {
    code = AdbcStatementExecuteQuery(&statement, NULL, NULL, &error);
    if (code == ADBC_STATUS_OK) {
      printf("✓ Success: NULL value inserted\n");
    } else {
      printf("✗ Failed: %s\n", error.message);
    }
  }

  array.release(&array);
  schema.release(&schema);
  AdbcStatementRelease(&statement, &error);

  // Cleanup
  printf("\nCleaning up...\n");
  AdbcConnectionRelease(&connection, &error);
  AdbcDatabaseRelease(&database, &error);

  printf("\n%s\n", "============================================================");
  printf("SUMMARY:\n");
  printf("Non-NULL parameters work correctly\n");
  printf("NULL parameters fail with Arrow type 'na' mapping error\n");
  printf("%s\n", "============================================================");

  return 0;
}
