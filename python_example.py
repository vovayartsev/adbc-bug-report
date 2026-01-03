#!/usr/bin/env python3
"""
Minimal reproducible example for NULL parameter binding issue in PostgreSQL ADBC driver.

Requirements:
    pip install adbc-driver-postgresql adbc-driver-manager pyarrow

Usage:
    python python_example.py

Expected: Should insert rows with NULL values successfully
Actual: Fails with "Can't map Arrow type 'na' to Postgres type"
"""

import adbc_driver_postgresql.dbapi as dbapi
import pyarrow  # Required by ADBC

def main():
    # Connect to PostgreSQL
    # Adjust connection string as needed
    conn = dbapi.connect("postgresql://user:changeme@localhost:5432/dbname")
    cursor = conn.cursor()

    # Create test table
    print("Creating test table...")
    cursor.execute("DROP TABLE IF EXISTS test_nulls")
    cursor.execute("""
        CREATE TABLE test_nulls (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)
    conn.commit()

    # Test 1: Insert with non-NULL values (this works)
    print("\nTest 1: Insert with non-NULL values...")
    try:
        cursor.execute(
            "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
            ("Alice", "alice@example.com")
        )
        conn.commit()
        print("✓ Success: Non-NULL values inserted")
    except Exception as e:
        print(f"✗ Failed: {e}")

    # Test 2: Insert with NULL value (this fails)
    print("\nTest 2: Insert with NULL value...")
    try:
        cursor.execute(
            "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
            ("Bob", None)  # None should be bound as NULL
        )
        conn.commit()
        print("✓ Success: NULL value inserted")
    except Exception as e:
        print(f"✗ Failed: {e}")
        print(f"   Error type: {type(e).__name__}")

    # Test 3: Multiple NULLs
    print("\nTest 3: Insert with multiple NULL values...")
    try:
        cursor.execute(
            "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
            (None, None)
        )
        conn.commit()
        print("✓ Success: Multiple NULL values inserted")
    except Exception as e:
        print(f"✗ Failed: {e}")

    # Show what was actually inserted
    print("\n--- Current table contents ---")
    cursor.execute("SELECT * FROM test_nulls")
    for row in cursor.fetchall():
        print(f"  {row}")

    # Cleanup
    cursor.execute("DROP TABLE test_nulls")
    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "="*60)
    print("SUMMARY:")
    print("Non-NULL parameters work correctly")
    print("NULL parameters fail with Arrow type 'na' mapping error")
    print("="*60)

if __name__ == "__main__":
    main()
