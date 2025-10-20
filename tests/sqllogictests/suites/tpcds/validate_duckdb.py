import duckdb
import re
import os
import decimal
import databend_driver
from rich import print

databend_port = os.getenv("QUERY_HTTP_HANDLER_PORT", "8000")

sf = 1
# Initialize a DuckDB instance
duckdb_location = "/tmp/tpcds.duck"
con = duckdb.connect(duckdb_location)
con.install_extension("tpcds")
con.load_extension("tpcds")

# Execute the commands
if not os.path.exists(duckdb_location) or os.path.getsize(duckdb_location) == 0:
    con.execute(f"CALL dsdgen(sf = {sf})")

### read queries.test
# Initialize a dictionary to store the queries and results
queries_dict = {}

# Initialize variables to store the current query number and text
query_num = None
query_text = ""

# Full queries_dict
for i in range(1, 100):
    with open(f"Q{i}", "r") as file:
        for line in file:
            if line.startswith("# Q"):
                # This is a query number line
                query_num = int(re.search(r"Q(\d+)", line).group(1))
            elif line.startswith("----"):
                queries_dict[query_num] = query_text
                query_text = ""
                # This is a result line, the next lines until an empty line will be the result
                for result_line in file:
                    if result_line.strip() == "":
                        break
                # Reset the query text
            elif line.startswith("statement") or line.startswith("query"):
                query_text = ""
            else:
                # This is a part of the query
                query_text += line


def compare_results(result, expected_result, num):
    if len(result) != len(expected_result):
        return False
    for r, er in zip(result, expected_result):
        if len(r) != len(er):
            print(f"Query {num} length checked error")
            return False
        for item_r, item_er in zip(r, er.values()):
            if (item_r == None or item_r == "") and (
                item_er == None or item_er == "NULL" or item_er == ""
            ):
                continue
            if isinstance(item_r, (decimal.Decimal, float, int)):
                if int(decimal.Decimal(item_r)) != int(decimal.Decimal(item_er)):
                    print(
                        f"Query {num} checked decimal error, not eq {item_r} !=  {item_er}"
                    )
                    print(r, er.values())
                    return False
            elif str(item_r) != str(item_er):
                print(r, er.values())
                print(
                    f"Query {num} checked string error, not eq {item_r} !=  {item_er}"
                )
                return False
    return True


from databend_driver import BlockingDatabendClient

client = BlockingDatabendClient(
    f"databend://root:@localhost:{databend_port}/tpcds?sslmode=disable"
)
databend_con = client.get_conn()

for i in range(1, 100):
    result = con.execute(f"PRAGMA tpcds({i})").fetchall()
    query = queries_dict[i]
    expected_result = databend_con.query_all(query)
    if not compare_results(result, expected_result, i):
        print(f"[red]Query Q{i} failed[/red]")
    else:
        print(f"[green]Query Q{i} passed[/green]")

# Close the connection
con.close()
