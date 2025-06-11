from datetime import datetime
from unicodedata import decimal

from fastavro import writer, parse_schema
from decimal import getcontext, Decimal
from datetime import datetime, date
import json
import uuid
from pprint import pprint

name = "nested_types"

getcontext().prec = 39
precs = [4, 39, 76]


row1 = {}
for p in precs:
    getcontext().prec = p
    row1["c_" + str(p) + "_2"] = Decimal("1" + "0" * (p - 3) + ".12")

row2 = {}
for p in precs:
    getcontext().prec = p
    row2["c_" + str(p) + "_2"] = Decimal("0.15")

row3 = {}
for p in precs:
    getcontext().prec = p
    row3["c_" + str(p) + "_2"] = Decimal("9" * (p - 3) + ".99")

decimals = [row1, row2, row3]
pprint(decimals)

datetime_fmt = "%Y-%m-%d %H:%M:%S.%f"
date_fmt = "%Y-%m-%d %H:%M:%S.%f"
ts = datetime.strptime("2025-05-20 01:02:03.12345", datetime_fmt)
dt = date(2025, 5, 20)
uuid_val = uuid.uuid4()

files = [
    (
        "decimal",
        [row1, row2, row3],
    ),
    (
        "extension",
        [
            {
                "c_date": dt,
                "c_uuid": uuid_val,
            },
        ],
    ),
    (
        "timestamp",
        [
            {
                "micros": ts,
                "micros_local": ts,
                "millis": ts,
                "millis_local": ts,
            },
        ],
    ),
]


if __name__ == "__main__":
    for name, rows in files:
        print(name)
        with open(name + ".avsc") as f:
            schema = json.load(f)

        parsed_schema = parse_schema(schema)
        with open(name + ".avro", "wb") as out:
            writer(out, parsed_schema, rows)
