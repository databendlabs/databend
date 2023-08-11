import pyarrow as pa
import pyarrow.flight


def str_cmp(s1: str, s2: str):
    if s1 == s2:
        return 0
    elif s1 < s2:
        return -1
    else:
        return 1


class FlightServer(pa.flight.FlightServerBase):
    def __init__(self, location="0.0.0.0:8815", **kwargs):
        super(FlightServer, self).__init__("grpc://" + location, **kwargs)

    def get_flight_info(self, context, descriptor):
        func_name = descriptor.path[0].decode("utf-8")
        # (string, string) -> int
        full_schema = pa.schema(
            [
                pa.field("arg1", pa.string(), nullable=False),
                pa.field("arg2", pa.string(), nullable=False),
                pa.field("output", pa.int32(), nullable=False),
            ]
        )
        return pa.flight.FlightInfo(
            schema=full_schema,
            descriptor=descriptor,
            endpoints=[],
            total_records=len(full_schema),
            total_bytes=0,
        )

    def do_exchange(self, context, descriptor, reader, writer):
        func_name = descriptor.path[0].decode("utf-8")
        print(f"do_exchange: {func_name}")
        print(reader.schema)
        output_schema = pa.schema([pa.field("output", pa.int32(), nullable=False)])

        writer.begin(output_schema)
        for chunk in reader:
            batch = chunk.data
            # string type is converted to LargeBinary in Datebend, thus we need cast it back to string here
            inputs = [[v.as_py() for v in array.cast(pa.string())] for array in batch]
            output = [
                str_cmp(*[col[i] for col in inputs]) for i in range(batch.num_rows)
            ]
            array = pa.array(output, type=pa.int32())
            output_batch = pa.RecordBatch.from_arrays([array], schema=output_schema)
            writer.write_batch(output_batch)


if __name__ == "__main__":
    server = FlightServer()
    server.serve()
