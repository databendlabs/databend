# Non-integer numeric text (fraction / exponent) loaded into integer columns must
# be converted exactly, not through f64 which loses precision beyond 2^53, and
# out-of-range values must be rejected instead of clamped to the type bounds.
# https://github.com/databendlabs/databend/issues/20786
import pytest

I64_MAX = 9223372036854775807
I64_MIN = -9223372036854775808
U64_MAX = 18446744073709551615

# (text, column type, numeric_cast_option, expected); expected None means the
# load must fail with an overflow error.
CASES = [
    # Beyond 2^53: every digit is kept.
    ("1760000000123456789.4", "bigint", "rounding", 1760000000123456789),
    ("1.760000000123456789e18", "bigint", "rounding", 1760000000123456789),
    ("9007199254740993.4", "bigint", "rounding", 9007199254740993),
    ("1760000000123456789.5", "bigint", "rounding", 1760000000123456790),
    ("1760000000123456789.4", "uint64", "rounding", 1760000000123456789),
    # Rounding is half away from zero.
    ("1.5", "bigint", "rounding", 2),
    ("-1.5", "bigint", "rounding", -2),
    ("-0.4", "uint64", "rounding", 0),
    # Near the type bounds.
    ("9223372036854775807.4", "bigint", "rounding", I64_MAX),
    ("-9223372036854775808.4", "bigint", "rounding", I64_MIN),
    ("18446744073709551615.4", "uint64", "rounding", U64_MAX),
    # Out of range: rejected, not clamped.
    ("-9223372036854775809", "bigint", "rounding", None),
    ("-9223372036854776000", "bigint", "rounding", None),
    ("9223372036854775807.5", "bigint", "rounding", None),
    ("-9223372036854775808.5", "bigint", "rounding", None),
    ("18446744073709551616", "uint64", "rounding", None),
    ("18446744073709551615.5", "uint64", "rounding", None),
    ("-1.5", "uint64", "rounding", None),
    # Truncating is toward zero.
    ("1760000000123456789.9", "bigint", "truncating", 1760000000123456789),
    ("-1.9", "bigint", "truncating", -1),
    ("-9223372036854775808.9", "bigint", "truncating", I64_MIN),
    ("1.9", "uint64", "truncating", 1),
    ("18446744073709551615.9", "uint64", "truncating", U64_MAX),
]

# How a single value is spelled as one row of each input format.
ROW_TEMPLATES = {
    "csv": "{}",
    "tsv": "{}",
    "ndjson": '{{"v":{}}}',
}

# Writes each selected string as one raw line: minimal quoting with a quote
# character and delimiter that never appear in the rows above.
RAW_LINE_FORMAT = (
    "file_format=(type=csv field_delimiter='|' quote='`' "
    "quote_style='quote_minimal' compression=none)"
)


@pytest.mark.parametrize("fmt", ROW_TEMPLATES.keys())
@pytest.mark.parametrize("text, typ, mode, expected", CASES)
def test_float_text_into_int(copy_env, fmt, text, typ, mode, expected):
    conn = copy_env.conn
    stage = f"@{copy_env.uniq_name}/"
    row = ROW_TEMPLATES[fmt].format(text)

    conn.exec(f"set numeric_cast_option = '{mode}'")
    conn.exec(f"copy into {stage} from (select '{row}') {RAW_LINE_FORMAT}")
    conn.exec(f"create or replace table t (v {typ})")

    copy_sql = f"copy into t from {stage} file_format=(type={fmt})"
    if expected is None:
        with pytest.raises(Exception, match="overflowed"):
            conn.exec(copy_sql)
        assert conn.query_row("select count(*) from t").values()[0] == 0
    else:
        conn.exec(copy_sql)
        assert conn.query_row("select v from t").values()[0] == expected
