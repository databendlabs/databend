import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

name = "nested_types"

files = [
    ("nested_record", [
        {"id": 0, "info": {"name": "yang", "contact": {"email": "yang@m", "phone": "911"}} },
        {"id": 1, "info": {"name": "wang", "contact": {"email": "wang@m"}} },
    ]),
    ("array", [
        {"tags": ["tall", "rich", "handsome"]},
        {"tags": []},
    ]),
    ("map", [
        {"scores": {"math": 100}},
        {"scores": {}},
    ]),
    ("number", [
        # todo:
        #  - inf,NaN
        #  - int to/from float, round
        {"c_int": ((1 << 31) - 1), "c_long": ((1 << 63) - 1), "c_float": 3.40282347e+38, "c_double": 1.7976931348623157E+308},
        {"c_int": -2**31, "c_long": -2 **63, "c_float": -3.40282347e+38, "c_double": -1.7976931348623157E+308},
    ]),
]

if __name__ == '__main__':
    for (name, rows) in files:
        print(name)
        schema = avro.schema.parse(open(f"{name}.avsc", "rb").read())
        writer = DataFileWriter(open(f"{name}.avro", "wb"), DatumWriter(), schema)
        for row in rows:
            writer.append(row)
        writer.close()

        reader = DataFileReader(open(f"{name}.avro", "rb"), DatumReader())
        for row in reader:
            print(row)
        reader.close()