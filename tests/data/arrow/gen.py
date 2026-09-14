import pyarrow as pa
import pyarrow.ipc as ipc


def main():
    table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})

    with open("sample.arrow", "wb") as f:
        with ipc.new_file(f, table.schema) as writer:
            writer.write_table(table)

    with open("sample.arrow_stream", "wb") as f:
        with ipc.new_stream(f, table.schema) as writer:
            writer.write_table(table)


if __name__ == "__main__":
    main()
