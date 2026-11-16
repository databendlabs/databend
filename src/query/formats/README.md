# Query formats

This crate implements field encoders, decoders, and output formats for external
text data. Stage loading also has format-specific readers under
`src/query/storages/stage/src/read/`.

## CSV output and input dialects

CSV output escapes a quote inside a quoted field by doubling it. Backslashes
are preserved literally. The `ESCAPE` file-format option is **load only**:
it changes neither output contents nor the decision to quote an output field.
This applies to both data and headers.

`QUOTE_STYLE` controls quoting of all non-NULL values:

- `quote_not_null` (the default) quotes every non-NULL value, including numbers,
  decimals, booleans, and BINARY.
- `quote_non_numeric` quotes every non-NULL value except numbers and decimals.
  Booleans, BINARY, dates/timestamps, and complex values are quoted.
- `quote_minimal` quotes a value when it is empty, equals `NULL_DISPLAY`, or
  contains the configured quote, field delimiter, or the record delimiter's
  first byte. An `ESCAPE` character alone does not require quoting.

Empty values and non-NULL values equal to `NULL_DISPLAY` are always quoted,
including numeric values under `quote_non_numeric`. NULL itself is written as
an unquoted `NULL_DISPLAY`. Headers follow the string/non-numeric quoting rules.
With the default load options, quoted empty STRING and BINARY fields remain
empty values, while unquoted empty fields are read as NULL.

Compatibility note: `quote_not_null` previously left numbers, decimals,
booleans, and non-empty BINARY unquoted. It now quotes these values as its name
specifies. Use `quote_non_numeric` to keep ordinary numbers and decimals
unquoted; unlike the old behavior, it also quotes booleans and BINARY.

To read this output without interpreting backslashes as escapes, use
`ESCAPE = ''` on load (the default). For example, a quoted field containing a
literal trailing backslash is written as `"tail\"`. A reader configured with
`ESCAPE = '\\'` may interpret the final `\"` as an embedded quote instead of a
field terminator, and continue reading into subsequent records. Within quoted
fields, that reader also consumes the backslash in `\t`, leaving `t`; it does
not convert the pair to a tab.

Using the same named file format for load and unload does not guarantee a
lossless round trip. Other options must also match the intended data semantics.
For example, loading with `ALLOW_QUOTED_NULLS = true` interprets quoted literal
NULL markers as NULL even though output quoted them for protection. Changing
`QUOTED_EMPTY_FIELD_AS` or enabling `TRIM_SPACE` can also change values on load.
Use a NULL marker without CSV delimiters, record delimiters, or quote characters;
NULL markers are emitted verbatim, without CSV quoting or escaping.

Outputting a backslash-escaped CSV dialect is not currently supported. Setting
`ESCAPE` does not opt into such an output dialect.
