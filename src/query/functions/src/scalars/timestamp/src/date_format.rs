// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::io::Write;
use std::sync::LazyLock;

use chrono::format::Item;
use chrono::format::StrftimeItems;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;

/// PostgreSQL to strftime format specifier mappings
///
/// The vector contains tuples of (postgres_format, strftime_format):
/// - For case-insensitive PostgreSQL formats (e.g., "YYYY"), any case variation will match
/// - For case-sensitive strftime formats (prefixed with '%'), exact case matching is required
///
/// Note: The sort order (by descending key length) is critical for correct pattern matching
static PG_STRFTIME_MAPPINGS: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    let mut mappings = vec![
        // ==============================================
        // Case-insensitive PostgreSQL format specifiers
        // (will match regardless of letter case)
        // ==============================================
        // Date components
        ("YYYY", "%Y"), // 4-digit year
        ("YY", "%y"),   // 2-digit year
        ("MMMM", "%B"), // Full month name
        ("MON", "%b"),  // Abbreviated month name (special word boundary handling)
        ("MM", "%m"),   // Month number (01-12)
        ("DD", "%d"),   // Day of month (01-31)
        ("DY", "%a"),   // Abbreviated weekday name
        // Time components
        ("HH24", "%H"), // 24-hour format (00-23)
        ("HH12", "%I"), // 12-hour format (01-12)
        ("AM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("PM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("MI", "%M"),   // Minutes (00-59)
        ("SS", "%S"),   // Seconds (00-59)
        ("FF", "%f"),   // Fractional seconds
        // Special cases
        ("UUUU", "%G"),    // ISO week-numbering year
        ("TZHTZM", "%z"),  // Timezone as ±HHMM
        ("TZH:TZM", "%z"), // Timezone as ±HH:MM
        ("TZH", "%:::z"),  // Timezone hour only
        // ==============================================
        // Case-sensitive strftime format specifiers
        // (must match exactly including case)
        // ==============================================
        ("%Y", "%Y"), // Year aliases
        ("%y", "%y"),
        ("%B", "%B"), // Month aliases
        ("%b", "%b"),
        ("%m", "%m"),
        ("%d", "%d"), // Day aliases
        ("%a", "%a"), // Weekday alias
        ("%H", "%H"), // Hour aliases
        ("%I", "%I"),
        ("%p", "%p"),       // AM/PM indicator
        ("%M", "%M"),       // Minute alias
        ("%S", "%S"),       // Second alias
        ("%f", "%f"),       // Fractional second alias
        ("%G", "%G"),       // ISO year alias
        ("%z", "%z"),       // Timezone aliases
        ("%:::z", "%:::z"), // Timezone hour alias
    ];

    // Critical: Sort by descending key length to ensure longest possible matches are found first
    // This prevents shorter patterns from incorrectly matching parts of longer patterns
    mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    mappings
});

static PG_KEY_LENGTHS: LazyLock<Vec<usize>> =
    LazyLock::new(|| PG_STRFTIME_MAPPINGS.iter().map(|(k, _)| k.len()).collect());

fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    text.chars()
        .zip(prefix.chars())
        .all(|(c1, c2)| c1.to_lowercase().eq(c2.to_lowercase()))
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[inline]
pub(super) fn pg_format_to_strftime(pg_format_string: &str) -> String {
    let mut result = String::with_capacity(pg_format_string.len() + 16);
    let mut current_byte_idx = 0;
    let format_len = pg_format_string.len();

    while current_byte_idx < format_len {
        let remaining_slice = &pg_format_string[current_byte_idx..];
        let mut matched = false;
        let first_char = remaining_slice.chars().next().unwrap_or('\0');

        for ((key, value), &key_len) in PG_STRFTIME_MAPPINGS.iter().zip(PG_KEY_LENGTHS.iter()) {
            if !key.is_empty() && !first_char.eq_ignore_ascii_case(&key.chars().next().unwrap()) {
                continue;
            }

            let is_case_sensitive_key = key.starts_with('%');
            let is_current_match = if is_case_sensitive_key {
                remaining_slice.starts_with(key)
            } else {
                starts_with_ignore_case(remaining_slice, key)
            };

            if is_current_match {
                let mut is_valid_match = true;
                if !is_case_sensitive_key && key.eq_ignore_ascii_case("MON") {
                    let next_byte_idx = current_byte_idx + key_len;

                    if current_byte_idx > 0 {
                        if let Some(prev_char) =
                            pg_format_string[..current_byte_idx].chars().next_back()
                        {
                            if is_word_char(prev_char) {
                                is_valid_match = false;
                            }
                        }
                    }

                    if is_valid_match && next_byte_idx < format_len {
                        if let Some(next_char) = pg_format_string[next_byte_idx..].chars().next() {
                            if is_word_char(next_char) {
                                is_valid_match = false;
                            }
                        }
                    }
                }

                if is_valid_match {
                    result.push_str(value);
                    current_byte_idx += key_len;
                    matched = true;
                    break;
                }
            }
        }

        if !matched {
            let c = first_char;
            result.push(c);
            current_byte_idx += c.len_utf8();
        }
    }

    result
}

// Keep locale-dependent formats stable across datetime backends.
fn replace_time_format(format: &str) -> Cow<'_, str> {
    if ["%c", "x", "X"].iter().any(|f| format.contains(f)) {
        let format = format
            .replace("%c", "%x %X")
            .replace("%x", "%F")
            .replace("%X", "%T");
        Cow::Owned(format)
    } else {
        Cow::Borrowed(format)
    }
}

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["date_format", "strftime", "to_char"]);
    registry.register_combine_nullable_2_arg::<TimestampType, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, StringType, NullableType<StringType>>(
            |micros, format, output, ctx| {
                let ts = timestamp_from_micros(micros, &ctx.func_ctx.tz);
                let format = prepare_format_string(format, &ctx.func_ctx.date_format_style);
                let items = StrftimeItems::new(&format).collect::<Vec<_>>();
                if items.iter().any(|item| matches!(item, Item::Error)) {
                    ctx.set_error(output.len(), format!("{format} is invalid time format"));
                    output.builder.commit_row();
                    output.validity.push(true);
                } else {
                    let rendered = ts.format_with_items(items.iter()).to_string();
                    output.builder.put_and_commit(rendered);
                    output.validity.push(true);
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, StringType>(|val, output, _| {
            write!(output.row_buffer, "{}", date_to_string(val)).unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampType, StringType>(|val, output, ctx| {
            write!(
                output.row_buffer,
                "{}",
                timestamp_to_string(val, &ctx.func_ctx.tz)
            )
            .unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampTzType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampTzType, StringType>(|val, output, _ctx| {
            write!(output.row_buffer, "{}", val).unwrap();
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<DateType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<DateType, NullableType<StringType>>(|val, output, _| {
            write!(output.builder.row_buffer, "{}", date_to_string(val)).unwrap();
            output.builder.commit_row();
            output.validity.push(true);
        }),
    );

    registry.register_combine_nullable_1_arg::<TimestampType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<TimestampType, NullableType<StringType>>(
            |val, output, ctx| {
                write!(
                    output.builder.row_buffer,
                    "{}",
                    timestamp_to_string(val, &ctx.func_ctx.tz)
                )
                .unwrap();
                output.builder.commit_row();
                output.validity.push(true);
            },
        ),
    );
}

fn prepare_format_string(format: &str, date_format_style: &str) -> String {
    let processed_format = if date_format_style == "oracle" {
        pg_format_to_strftime(format)
    } else {
        format.to_string()
    };
    replace_time_format(&processed_format).to_string()
}
