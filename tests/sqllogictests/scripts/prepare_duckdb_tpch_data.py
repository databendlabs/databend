import duckdb, sys

sf = sys.argv[1]

# Initialize a DuckDB instance
con = duckdb.connect(":memory:")

con.install_extension("tpch")
con.load_extension("tpch")
# Execute the commands
con.execute(f"CALL dbgen(sf={sf})")
con.execute(f"EXPORT DATABASE '/tmp/tpch_{sf}/' (FORMAT CSV, DELIMITER '|')")

# Close the connection
con.close()
