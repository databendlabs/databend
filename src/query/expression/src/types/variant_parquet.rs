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
use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::Timelike;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use ethnum::i256;
use jsonb::Date as JsonbDate;
use jsonb::Decimal64 as JsonbDecimal64;
use jsonb::Decimal128 as JsonbDecimal128;
use jsonb::Decimal256 as JsonbDecimal256;
use jsonb::Number as JsonbNumber;
use jsonb::Timestamp as JsonbTimestamp;
use jsonb::TimestampTz as JsonbTimestampTz;
use jsonb::Value as JsonbValue;
use parquet_variant::ObjectFieldBuilder;
use parquet_variant::Variant;
use parquet_variant::VariantBuilder;
use parquet_variant::VariantBuilderExt;
use parquet_variant::VariantDecimal8;
use parquet_variant::VariantDecimal16;

const METADATA_LEN_BYTES: usize = 4;

pub fn jsonb_to_parquet_variant_bytes(jsonb: &[u8]) -> Result<Vec<u8>> {
    let (metadata, value) = jsonb_to_parquet_variant_parts(jsonb)?;
    Ok(encode_parquet_variant_bytes(&metadata, &value))
}

pub fn parquet_variant_bytes_to_jsonb(bytes: &[u8]) -> Result<Vec<u8>> {
    let (metadata, value) = decode_parquet_variant_bytes(bytes)?;
    parquet_variant_to_jsonb(metadata, value)
}

pub fn jsonb_to_parquet_variant_parts(jsonb: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let value = jsonb::from_slice(jsonb).map_err(|e| {
        ErrorCode::Internal(format!(
            "Failed to decode JSONB value for variant encoding: {e}"
        ))
    })?;
    jsonb_value_to_parquet_variant_parts_with_field_names(&value, &[])
}

pub fn parquet_variant_to_jsonb(metadata: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    if metadata.is_empty() {
        return Ok(crate::types::variant::JSONB_NULL.to_vec());
    }
    let variant =
        Variant::try_new(metadata, value).map_err(|e| ErrorCode::Internal(e.to_string()))?;
    variant_to_jsonb_bytes(variant)
}

pub fn jsonb_value_to_parquet_variant_parts_with_field_names(
    value: &JsonbValue<'_>,
    field_names: &[String],
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut builder = VariantBuilder::new();
    if !field_names.is_empty() {
        builder = builder.with_field_names(field_names.iter().map(|name| name.as_str()));
    }
    append_jsonb_value(&mut builder, value)?;
    let (metadata, value) = builder.finish();
    Ok((metadata, value))
}

fn encode_parquet_variant_bytes(metadata: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(METADATA_LEN_BYTES + metadata.len() + value.len());
    buf.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    buf.extend_from_slice(metadata);
    buf.extend_from_slice(value);
    buf
}

fn decode_parquet_variant_bytes(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    if bytes.len() < METADATA_LEN_BYTES {
        return Err(ErrorCode::Internal(
            "Invalid parquet variant bytes: missing metadata length".to_string(),
        ));
    }
    let mut len_buf = [0_u8; METADATA_LEN_BYTES];
    len_buf.copy_from_slice(&bytes[..METADATA_LEN_BYTES]);
    let metadata_len = u32::from_le_bytes(len_buf) as usize;
    let total_len = bytes.len();
    if METADATA_LEN_BYTES + metadata_len > total_len {
        return Err(ErrorCode::Internal(
            "Invalid parquet variant bytes: metadata length out of bounds".to_string(),
        ));
    }
    let metadata = &bytes[METADATA_LEN_BYTES..(METADATA_LEN_BYTES + metadata_len)];
    let value = &bytes[(METADATA_LEN_BYTES + metadata_len)..];
    Ok((metadata, value))
}

fn append_jsonb_value(builder: &mut impl VariantBuilderExt, value: &JsonbValue) -> Result<()> {
    match value {
        JsonbValue::Null => builder.append_value(Variant::Null),
        JsonbValue::Bool(v) => builder.append_value(*v),
        JsonbValue::Number(n) => append_jsonb_number(builder, n)?,
        JsonbValue::String(s) => builder.append_value(s.as_ref()),
        JsonbValue::Binary(bytes) => builder.append_value(Variant::Binary(bytes)),
        JsonbValue::Date(JsonbDate { value }) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(Duration::days(*value as i64)))
                .ok_or_else(|| {
                    ErrorCode::Internal("Invalid date value for variant encoding".to_string())
                })?;
            builder.append_value(Variant::Date(date));
        }
        JsonbValue::Timestamp(JsonbTimestamp { value }) => {
            let (secs, nanos) = split_micros(*value);
            let dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
                ErrorCode::Internal("Invalid timestamp value for variant encoding".to_string())
            })?;
            builder.append_value(Variant::TimestampMicros(dt));
        }
        JsonbValue::TimestampTz(JsonbTimestampTz { value, .. }) => {
            let (secs, nanos) = split_micros(*value);
            let dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
                ErrorCode::Internal("Invalid timestamp value for variant encoding".to_string())
            })?;
            builder.append_value(Variant::TimestampMicros(dt));
        }
        JsonbValue::Interval(_) => {
            return Err(ErrorCode::Unimplemented(
                "Interval is not supported in parquet variant encoding".to_string(),
            ));
        }
        JsonbValue::Array(values) => {
            let mut list_builder = builder
                .try_new_list()
                .map_err(|e| ErrorCode::Internal(e.to_string()))?;
            for item in values {
                append_jsonb_value(&mut list_builder, item)?;
            }
            list_builder.finish();
        }
        JsonbValue::Object(obj) => {
            let mut obj_builder = builder
                .try_new_object()
                .map_err(|e| ErrorCode::Internal(e.to_string()))?;
            for (key, item) in obj.iter() {
                let mut field_builder = ObjectFieldBuilder::new(key, &mut obj_builder);
                append_jsonb_value(&mut field_builder, item)?;
            }
            obj_builder.finish();
        }
    }
    Ok(())
}

fn append_jsonb_number(builder: &mut impl VariantBuilderExt, number: &JsonbNumber) -> Result<()> {
    match number {
        JsonbNumber::Int64(v) => builder.append_value(*v),
        JsonbNumber::UInt64(v) => {
            if *v <= i64::MAX as u64 {
                builder.append_value(*v as i64);
            } else {
                let decimal = VariantDecimal16::try_new(*v as i128, 0)
                    .map_err(|e| ErrorCode::Internal(e.to_string()))?;
                builder.append_value(decimal);
            }
        }
        JsonbNumber::Float64(v) => builder.append_value(*v),
        JsonbNumber::Decimal64(JsonbDecimal64 { value, scale }) => {
            let decimal = VariantDecimal8::try_new(*value, *scale)
                .map_err(|e| ErrorCode::Internal(e.to_string()))?;
            builder.append_value(decimal);
        }
        JsonbNumber::Decimal128(JsonbDecimal128 { value, scale }) => {
            let decimal = VariantDecimal16::try_new(*value, *scale)
                .map_err(|e| ErrorCode::Internal(e.to_string()))?;
            builder.append_value(decimal);
        }
        JsonbNumber::Decimal256(JsonbDecimal256 { value, scale }) => {
            let min = i256::from(i128::MIN);
            let max = i256::from(i128::MAX);
            if *value < min || *value > max {
                return Err(ErrorCode::Unimplemented(
                    "Decimal256 is out of range for parquet variant encoding".to_string(),
                ));
            }
            let decimal = VariantDecimal16::try_new(value.as_i128(), *scale)
                .map_err(|e| ErrorCode::Internal(e.to_string()))?;
            builder.append_value(decimal);
        }
    }
    Ok(())
}

fn split_micros(value: i64) -> (i64, u32) {
    let secs = value.div_euclid(1_000_000);
    let nanos = value.rem_euclid(1_000_000) as u32 * 1_000;
    (secs, nanos)
}

struct JsonbArena {
    binaries: Vec<Vec<u8>>,
}

impl JsonbArena {
    fn new() -> Self {
        Self {
            binaries: Vec::new(),
        }
    }

    fn alloc_binary<'a>(&'a mut self, bytes: &[u8]) -> &'a [u8] {
        self.binaries.push(bytes.to_vec());
        self.binaries.last().unwrap().as_slice()
    }
}

fn variant_to_jsonb_bytes(variant: Variant<'_, '_>) -> Result<Vec<u8>> {
    let mut arena = JsonbArena::new();
    let value = variant_to_jsonb_value(variant, &mut arena)?;
    let mut buf = Vec::new();
    value.write_to_vec(&mut buf);
    Ok(buf)
}

fn variant_to_jsonb_value<'a>(
    variant: Variant<'_, '_>,
    arena: &'a mut JsonbArena,
) -> Result<JsonbValue<'a>> {
    let arena_ptr: *mut JsonbArena = arena;
    unsafe { variant_to_jsonb_value_with_arena(variant, arena_ptr) }
}

unsafe fn variant_to_jsonb_value_with_arena<'a>(
    variant: Variant<'_, '_>,
    arena: *mut JsonbArena,
) -> Result<JsonbValue<'a>> {
    let value = match variant {
        Variant::Null => JsonbValue::Null,
        Variant::BooleanTrue => JsonbValue::Bool(true),
        Variant::BooleanFalse => JsonbValue::Bool(false),
        Variant::Int8(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
        Variant::Int16(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
        Variant::Int32(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
        Variant::Int64(v) => JsonbValue::Number(JsonbNumber::Int64(v)),
        Variant::Float(v) => JsonbValue::Number(JsonbNumber::Float64(v as f64)),
        Variant::Double(v) => JsonbValue::Number(JsonbNumber::Float64(v)),
        Variant::Decimal4(v) => JsonbValue::Number(JsonbNumber::Decimal64(JsonbDecimal64 {
            scale: v.scale(),
            value: v.integer() as i64,
        })),
        Variant::Decimal8(v) => JsonbValue::Number(JsonbNumber::Decimal64(JsonbDecimal64 {
            scale: v.scale(),
            value: v.integer(),
        })),
        Variant::Decimal16(v) => JsonbValue::Number(JsonbNumber::Decimal128(JsonbDecimal128 {
            scale: v.scale(),
            value: v.integer(),
        })),
        Variant::Date(v) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = v.signed_duration_since(epoch).num_days();
            JsonbValue::Date(JsonbDate { value: days as i32 })
        }
        Variant::TimestampMicros(v) => JsonbValue::Timestamp(JsonbTimestamp {
            value: v.timestamp_micros(),
        }),
        Variant::TimestampNtzMicros(v) => JsonbValue::Timestamp(JsonbTimestamp {
            value: v.and_utc().timestamp_micros(),
        }),
        Variant::TimestampNanos(v) => JsonbValue::Timestamp(JsonbTimestamp {
            value: timestamp_nanos_to_micros(v.timestamp(), v.timestamp_subsec_nanos()),
        }),
        Variant::TimestampNtzNanos(v) => {
            let utc = v.and_utc();
            JsonbValue::Timestamp(JsonbTimestamp {
                value: timestamp_nanos_to_micros(utc.timestamp(), utc.timestamp_subsec_nanos()),
            })
        }
        Variant::Binary(v) => JsonbValue::Binary(unsafe { (*arena).alloc_binary(v) }),
        Variant::String(v) => JsonbValue::String(Cow::Owned(v.to_string())),
        Variant::ShortString(v) => JsonbValue::String(Cow::Owned(v.as_str().to_string())),
        Variant::Uuid(v) => JsonbValue::String(Cow::Owned(v.to_string())),
        Variant::Time(v) => JsonbValue::String(Cow::Owned(format!(
            "{:02}:{:02}:{:02}.{:06}",
            v.hour(),
            v.minute(),
            v.second(),
            v.nanosecond() / 1_000
        ))),
        Variant::Object(obj) => {
            let mut map = BTreeMap::new();
            for (key, val) in obj.iter() {
                let jsonb_val = unsafe { variant_to_jsonb_value_with_arena(val, arena) }?;
                map.insert(key.to_string(), jsonb_val);
            }
            JsonbValue::Object(map)
        }
        Variant::List(list) => {
            let mut values = Vec::with_capacity(list.len());
            for item in list.iter() {
                values.push(unsafe { variant_to_jsonb_value_with_arena(item, arena) }?);
            }
            JsonbValue::Array(values)
        }
    };
    Ok(value)
}

fn timestamp_nanos_to_micros(secs: i64, nanos: u32) -> i64 {
    secs.saturating_mul(1_000_000)
        .saturating_add((nanos / 1_000) as i64)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use jsonb::Date as JsonbDate;
    use jsonb::Decimal64 as JsonbDecimal64;
    use jsonb::Decimal128 as JsonbDecimal128;
    use jsonb::Interval as JsonbInterval;
    use jsonb::Number as JsonbNumber;
    use jsonb::Timestamp as JsonbTimestamp;
    use jsonb::Value as JsonbValue;

    use super::*;

    fn assert_roundtrip(value: JsonbValue<'static>) {
        let mut buf = Vec::new();
        value.clone().write_to_vec(&mut buf);
        let encoded = jsonb_to_parquet_variant_bytes(&buf).unwrap();
        let decoded = parquet_variant_bytes_to_jsonb(&encoded).unwrap();
        let decoded_value = jsonb::from_slice(&decoded).unwrap();
        assert_eq!(value, decoded_value);
    }

    #[test]
    fn test_variant_roundtrip_basic() {
        assert_roundtrip(JsonbValue::Null);
        assert_roundtrip(JsonbValue::Bool(true));
        assert_roundtrip(JsonbValue::Number(JsonbNumber::Int64(-42)));
        assert_roundtrip(JsonbValue::Number(JsonbNumber::UInt64(42)));
        assert_roundtrip(JsonbValue::Number(JsonbNumber::Float64(
            std::f64::consts::PI,
        )));
        assert_roundtrip(JsonbValue::Number(JsonbNumber::Decimal64(JsonbDecimal64 {
            value: 12345,
            scale: 2,
        })));
        assert_roundtrip(JsonbValue::Number(JsonbNumber::Decimal128(
            JsonbDecimal128 {
                value: 123456789,
                scale: 4,
            },
        )));
        assert_roundtrip(JsonbValue::String(Cow::Owned("hello".to_string())));
        assert_roundtrip(JsonbValue::Binary(b"bin"));
        assert_roundtrip(JsonbValue::Date(JsonbDate { value: 10 }));
        assert_roundtrip(JsonbValue::Timestamp(JsonbTimestamp { value: 123456 }));
        assert_roundtrip(JsonbValue::Array(vec![
            JsonbValue::Null,
            JsonbValue::Bool(false),
            JsonbValue::Number(JsonbNumber::Int64(7)),
        ]));

        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), JsonbValue::Number(JsonbNumber::Int64(1)));
        assert_roundtrip(JsonbValue::Object(obj));
    }

    #[test]
    fn test_variant_interval_not_supported() {
        let value = JsonbValue::Interval(JsonbInterval {
            months: 1,
            days: 2,
            micros: 3,
        });
        let mut buf = Vec::new();
        value.write_to_vec(&mut buf);
        let err = jsonb_to_parquet_variant_bytes(&buf).unwrap_err();
        assert!(err.message().contains("Interval"));
    }
}
