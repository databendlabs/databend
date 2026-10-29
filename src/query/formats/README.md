# Query formats

This crate implements field encoders, decoders, and output formats for external
text data. Stage loading also has format-specific readers under
`src/query/storages/stage/src/read/`.

## CSV output and input dialects

CSV output escapes a quote inside a quoted field by doubling it. Backslashes
are preserved literally. The `ESCAPE` file-format option is **load only**:
it changes neither output contents nor the decision to quote an output field.
This applies to both data and headers.

For string-encoded values and headers:

- `QUOTE_STYLE = 'quote_not_null'` always adds quotes.
- `QUOTE_STYLE = 'quote_minimal'` adds quotes when the value contains the
  configured quote, field delimiter, or the record delimiter's first byte. An
  `ESCAPE` character alone does not require quoting.

To read this output without interpreting backslashes as escapes, use
`ESCAPE = ''` on load (the default). For example, a quoted field containing a
literal trailing backslash is written as `"tail\"`. A reader configured with
`ESCAPE = '\\'` may interpret the final `\"` as an embedded quote instead of a
field terminator, and continue reading into subsequent records. Within quoted
fields, that reader also consumes the backslash in `\t`, leaving `t`; it does
not convert the pair to a tab.

Using the same named file format for load and unload does not guarantee a
lossless round trip. Other options must also match the intended data semantics.
For example, minimal quoting can make an empty string indistinguishable from
an empty field representing NULL, or a literal `NULL_DISPLAY` string
indistinguishable from NULL. Disabling input escaping does not resolve those
collisions.

Outputting a backslash-escaped CSV dialect is not currently supported. Setting
`ESCAPE` does not opt into such an output dialect.
