#!/usr/bin/env elixir
#
# Minimal reproducible example for NULL parameter binding issue in PostgreSQL ADBC driver.
#
# Requirements:
#   Add to mix.exs: {:adbc, "~> 0.7"}
#   Config: config :adbc, :drivers, [:postgresql]
#
# Usage:
#   elixir elixir_example.exs
#
# Expected: Should insert rows with NULL values successfully
# Actual: Fails with "Can't map Arrow type 'na' to Postgres type"
#

Mix.install([
  {:adbc, "~> 0.7"}
])

Application.put_env(:adbc, :drivers, [:postgresql])

defmodule NullParameterTest do
  def run do
    # Adjust connection URI as needed
    db_opts = [
      driver: :postgresql,
      uri: "postgresql://user:password@localhost:5432/dbname"
    ]

    {:ok, db} = Adbc.Database.start_link(db_opts)
    {:ok, conn} = Adbc.Connection.start_link(database: db)

    # Create test table
    IO.puts("Creating test table...")
    {:ok, _} = Adbc.Connection.query(conn, "DROP TABLE IF EXISTS test_nulls")
    {:ok, _} = Adbc.Connection.query(conn, """
      CREATE TABLE test_nulls (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT
      )
    """)

    # Test 1: Insert with non-NULL values (this works)
    IO.puts("\nTest 1: Insert with non-NULL values...")
    case Adbc.Connection.query(conn,
           "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
           ["Alice", "alice@example.com"]) do
      {:ok, _} -> IO.puts("✓ Success: Non-NULL values inserted")
      {:error, error} -> IO.puts("✗ Failed: #{inspect(error)}")
    end

    # Test 2: Insert with NULL value (this fails)
    IO.puts("\nTest 2: Insert with NULL value...")
    case Adbc.Connection.query(conn,
           "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
           ["Bob", nil]) do
      {:ok, _} -> IO.puts("✓ Success: NULL value inserted")
      {:error, error} ->
        IO.puts("✗ Failed: #{inspect(error)}")
        IO.puts("   Message: #{error.message}")
    end

    # Test 3: Multiple NULLs
    IO.puts("\nTest 3: Insert with multiple NULL values...")
    case Adbc.Connection.query(conn,
           "INSERT INTO test_nulls (name, email) VALUES ($1, $2)",
           [nil, nil]) do
      {:ok, _} -> IO.puts("✓ Success: Multiple NULL values inserted")
      {:error, error} -> IO.puts("✗ Failed: #{inspect(error)}")
    end

    # Show what was actually inserted
    IO.puts("\n--- Current table contents ---")
    case Adbc.Connection.query(conn, "SELECT * FROM test_nulls") do
      {:ok, result} ->
        result
        |> Adbc.Result.materialize()
        |> then(fn %{data: data} ->
          Enum.each(data, fn col ->
            IO.puts("  #{col.name}: #{inspect(col.data)}")
          end)
        end)
      {:error, error} ->
        IO.puts("  Error reading results: #{inspect(error)}")
    end

    # Cleanup
    Adbc.Connection.query(conn, "DROP TABLE test_nulls")

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("SUMMARY:")
    IO.puts("Non-NULL parameters work correctly")
    IO.puts("NULL parameters fail with Arrow type 'na' mapping error")
    IO.puts(String.duplicate("=", 60))
  end
end

NullParameterTest.run()
