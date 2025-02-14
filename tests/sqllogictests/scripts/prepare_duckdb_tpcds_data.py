import duckdb, sys

sf = sys.argv[1]

# Initialize a DuckDB instance
con = duckdb.connect(":memory:")

con.install_extension("tpcds")
con.load_extension("tpcds")
# Execute the commands
con.execute(f"CALL dsdgen(sf={sf})")
con.execute(f"EXPORT DATABASE '/tmp/tpcds_{sf}/' (FORMAT CSV, DELIMITER '|')")

# Close the connection
con.close()
