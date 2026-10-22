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

//! Borrowed access to JSON DOMs used by the NDJSON field decoder.
//!
//! Keep the numeric token, rather than converting through f64. In particular,
//! DECIMAL must see all the digits in the file, including in nested values.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;

pub enum JsonNumber<'a> {
    Serde(&'a serde_json::Number),
    Sonic(sonic_rs::RawNumber),
}

impl JsonNumber<'_> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Serde(n) => n.as_str(),
            Self::Sonic(n) => n.as_str(),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Serde(n) => n.as_i64(),
            Self::Sonic(n) => n.as_str().parse().ok(),
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Serde(n) => n.as_u64(),
            Self::Sonic(n) => {
                // serde_json 1.0.151 normalizes the integer token -0 to 0.
                if n.as_str() == "-0" {
                    Some(0)
                } else {
                    n.as_str().parse().ok()
                }
            }
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Serde(n) => n.as_f64(),
            Self::Sonic(n) => {
                let text = if n.as_str() == "-0" { "0" } else { n.as_str() };
                text.parse::<f64>().ok().filter(|v| v.is_finite())
            }
        }
    }

    /// Match serde's lexical normalization for NUMBER -> STRING (e.g. 1E3).
    pub fn string_text(&self) -> serde_json::Result<Cow<'_, str>> {
        match self {
            Self::Serde(n) => Ok(Cow::Borrowed(n.as_str())),
            Self::Sonic(n) => {
                let text = n.as_str();
                if text == "-0" {
                    return Ok(Cow::Borrowed("0"));
                }
                if memchr::memchr2(b'e', b'E', text.as_bytes()).is_none() {
                    return Ok(Cow::Borrowed(text));
                }
                Ok(Cow::Owned(
                    serde_json::from_str::<serde_json::Number>(text)?.to_string(),
                ))
            }
        }
    }
}

impl Display for JsonNumber<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string_text().map_err(|_| std::fmt::Error)?)
    }
}

/// Monomorphized access: no second DOM, boxed iterators, or dynamic dispatch.
/// Sonic values must be constructed with `parse_sonic` to retain number text.
pub trait JsonValue: Display + Sized {
    const OBJECT_LOOKUP_IS_LINEAR: bool;

    fn is_null(&self) -> bool;
    fn as_bool(&self) -> Option<bool>;
    fn as_str(&self) -> Option<&str>;
    fn as_number(&self) -> Option<JsonNumber<'_>>;
    fn as_array(&self) -> Option<&[Self]>;
    fn object_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>>;
    fn object_get(&self, key: &str) -> Option<&Self>;
    fn write_jsonb(&self, out: &mut Vec<u8>) -> serde_json::Result<()>;

    fn diagnostic_text(&self) -> String {
        self.to_string()
    }
}

/// Reusable field lookup for one row or named tuple. Build an index only when
/// repeated linear scans would be expensive. Exact-match keys borrow the DOM;
/// case-insensitive keys are normalized once per object, not once per scan.
pub struct JsonObjectLookup<'a, V> {
    value: &'a V,
    case_sensitive: bool,
    index: Option<HashMap<Cow<'a, str>, &'a V>>,
}

impl<'a, V: JsonValue> JsonObjectLookup<'a, V> {
    /// COPY has historically indexed case-folded keys with last-match wins.
    pub fn for_row(value: &'a V, case_sensitive: bool, lookups: usize) -> Self {
        Self::new(value, case_sensitive, lookups, true)
    }

    /// Named tuples have historically used the first case-folded match.
    pub fn for_tuple(value: &'a V, case_sensitive: bool, lookups: usize) -> Self {
        Self::new(value, case_sensitive, lookups, false)
    }

    fn new(value: &'a V, case_sensitive: bool, lookups: usize, row: bool) -> Self {
        // Below either bound, total scan work is bounded by a small constant
        // times the object/lookup count. Do not index a wide projected object
        // when only a handful of its fields will actually be read.
        const MIN_FIELDS: usize = 16;
        const MIN_LOOKUPS: usize = 8;
        let mut index = None;
        if let Some(object) = value.object_iter() {
            let repeated_scan = object.len() >= MIN_FIELDS && lookups >= MIN_LOOKUPS;
            let needs_index = if case_sensitive {
                V::OBJECT_LOOKUP_IS_LINEAR && repeated_scan
            } else {
                row || repeated_scan
            };
            if needs_index {
                let mut values = HashMap::with_capacity(object.len());
                for (key, value) in object {
                    let key = if case_sensitive {
                        Cow::Borrowed(key)
                    } else {
                        Cow::Owned(key.to_lowercase())
                    };
                    if row {
                        values.insert(key, value);
                    } else {
                        values.entry(key).or_insert(value);
                    }
                }
                index = Some(values);
            }
        }
        Self {
            value,
            case_sensitive,
            index,
        }
    }

    // Keep the already-indexed Serde branch a direct lookup even at opt-level=s.
    #[inline(always)]
    pub fn get(&self, field_name: &str) -> Option<&'a V> {
        if self.case_sensitive {
            // Serde already has an index: keep its hot lookup path direct.
            if !V::OBJECT_LOOKUP_IS_LINEAR {
                return self.value.object_get(field_name);
            }
            return match &self.index {
                Some(index) => index.get(field_name).copied(),
                None => self.value.object_get(field_name),
            };
        }
        let field_name = field_name.to_lowercase();
        match &self.index {
            Some(index) => index.get(field_name.as_str()).copied(),
            None => self
                .value
                .object_iter()?
                .find_map(|(key, value)| (key.to_lowercase() == field_name).then_some(value)),
        }
    }
}

impl JsonValue for serde_json::Value {
    const OBJECT_LOOKUP_IS_LINEAR: bool = false;

    fn is_null(&self) -> bool {
        self.is_null()
    }

    fn as_bool(&self) -> Option<bool> {
        self.as_bool()
    }

    fn as_str(&self) -> Option<&str> {
        self.as_str()
    }

    fn as_number(&self) -> Option<JsonNumber<'_>> {
        self.as_number().map(JsonNumber::Serde)
    }

    fn as_array(&self) -> Option<&[Self]> {
        self.as_array().map(Vec::as_slice)
    }

    fn object_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>> {
        self.as_object()
            .map(|o| o.iter().map(|(k, v)| (k.as_str(), v)))
    }

    fn object_get(&self, key: &str) -> Option<&Self> {
        self.as_object()?.get(key)
    }

    fn write_jsonb(&self, out: &mut Vec<u8>) -> serde_json::Result<()> {
        jsonb::Value::from(self).write_to_vec(out);
        Ok(())
    }
}

impl JsonValue for sonic_rs::Value {
    const OBJECT_LOOKUP_IS_LINEAR: bool = true;

    fn is_null(&self) -> bool {
        sonic_rs::JsonValueTrait::is_null(self)
    }

    fn as_bool(&self) -> Option<bool> {
        sonic_rs::JsonValueTrait::as_bool(self)
    }

    fn as_str(&self) -> Option<&str> {
        sonic_rs::JsonValueTrait::as_str(self)
    }

    fn as_number(&self) -> Option<JsonNumber<'_>> {
        sonic_rs::JsonValueTrait::as_raw_number(self).map(JsonNumber::Sonic)
    }

    fn as_array(&self) -> Option<&[Self]> {
        sonic_rs::JsonContainerTrait::as_array(self).map(|a| a.as_slice())
    }

    fn object_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>> {
        sonic_rs::JsonContainerTrait::as_object(self).map(|o| o.iter())
    }

    fn object_get(&self, key: &str) -> Option<&Self> {
        sonic_rs::JsonContainerTrait::as_object(self)?.get(&key)
    }

    fn write_jsonb(&self, out: &mut Vec<u8>) -> serde_json::Result<()> {
        sonic_to_jsonb(self).write_to_vec(out);
        Ok(())
    }

    fn diagnostic_text(&self) -> String {
        // Only on the error path: preserve Serde's canonical number spelling.
        serde_json::from_str::<serde_json::Value>(&self.to_string())
            .map(|value| value.to_string())
            .unwrap_or_else(|_| self.to_string())
    }
}

/// Convert directly without serializing JSON text or constructing a Serde DOM.
/// Strings borrow the arena until encoding finishes; object keys are owned by
/// jsonb's sorted object map, just as in its Serde conversion.
fn sonic_to_jsonb(value: &sonic_rs::Value) -> jsonb::Value<'_> {
    if let Some(s) = JsonValue::as_str(value) {
        jsonb::Value::String(Cow::Borrowed(s))
    } else if let Some(number) = JsonValue::as_number(value) {
        sonic_number_to_jsonb(&number)
    } else if let Some(b) = JsonValue::as_bool(value) {
        jsonb::Value::Bool(b)
    } else if let Some(array) = JsonValue::as_array(value) {
        jsonb::Value::Array(array.iter().map(sonic_to_jsonb).collect())
    } else if let Some(object) = JsonValue::object_iter(value) {
        jsonb::Value::Object(
            object
                .map(|(key, value)| (key.to_owned(), sonic_to_jsonb(value)))
                .collect(),
        )
    } else {
        jsonb::Value::Null
    }
}

/// Match jsonb's From<&serde_json::Value> numeric policy, not its text parser's
/// potentially different decimal policy. Fractional/exponent tokens remain f64;
/// only wide integer tokens use Decimal128/256, and nonfinite values become null.
fn sonic_number_to_jsonb(number: &JsonNumber<'_>) -> jsonb::Value<'static> {
    let text = number.as_str();
    if memchr::memchr3(b'.', b'e', b'E', text.as_bytes()).is_none() {
        if let Some(n) = number.as_u64() {
            return jsonb::Value::Number(jsonb::Number::UInt64(n));
        }
        if let Some(n) = number.as_i64() {
            return jsonb::Value::Number(jsonb::Number::Int64(n));
        }
        if let Ok(n) = text.parse::<i128>() {
            // jsonb's Decimal128 coefficient is limited to 38 digits, not the
            // full i128 range. The dependency is pinned; boundary tests below
            // compare encoded bytes to its Serde conversion on upgrades.
            const MAX_DECIMAL128: i128 = 10_i128.pow(38) - 1;
            let number = if (-MAX_DECIMAL128..=MAX_DECIMAL128).contains(&n) {
                jsonb::Number::Decimal128(jsonb::Decimal128 { value: n, scale: 0 })
            } else {
                jsonb::Number::Decimal256(jsonb::Decimal256 {
                    value: n.into(),
                    scale: 0,
                })
            };
            return jsonb::Value::Number(number);
        }
        if let Ok(n) = text.parse::<u128>() {
            return jsonb::Value::Number(jsonb::Number::Decimal256(jsonb::Decimal256 {
                value: n.into(),
                scale: 0,
            }));
        }
    }
    match number.as_f64() {
        Some(n) => jsonb::Value::Number(jsonb::Number::Float64(n)),
        None => jsonb::Value::Null,
    }
}

/// Parse one complete, validated JSON row with exact numeric tokens. `None`
/// requests the caller's serde fallback, including its established diagnostics.
pub fn parse_sonic(data: &[u8]) -> Option<sonic_rs::Value> {
    if !within_sonic_depth_limit(data) {
        return None;
    }
    // arbitrary_precision retains number tokens. Use from_slice rather than
    // Deserializer::deserialize, which does not check trailing input.
    let value: sonic_rs::Value = sonic_rs::from_slice(data).ok()?;
    // Duplicate keys deliberately retain the backend's native behavior. Only
    // serde's reserved number key requires DOM inspection. Escapes may hide the
    // key's spelling; ordinary rows need no post-parse object traversal.
    if memchr::memchr2(b'$', b'\\', data).is_some() && contains_serde_number_key(&value) {
        return None;
    }
    Some(value)
}

/// serde_json::Value treats this key as an arbitrary-precision number wrapper,
/// even in otherwise ordinary JSON objects. Keep that behavior independent of
/// the deliberately unspecified duplicate-key policy.
fn contains_serde_number_key(value: &sonic_rs::Value) -> bool {
    if let Some(object) = sonic_rs::JsonContainerTrait::as_object(value) {
        object.iter().any(|(key, value)| {
            key == "$serde_json::private::Number" || contains_serde_number_key(value)
        })
    } else if let Some(array) = value.as_array() {
        array.iter().any(contains_serde_number_key)
    } else {
        false
    }
}

/// Bound nesting *before* entering Sonic's recursive DOM parser. Also retain
/// serde's depth-limit behavior by falling back at 32 levels (well before
/// serde's limit). Sonic needs more stack per level, especially in debug builds.
/// The SIMD opening-delimiter count handles
/// ordinary rows; only large/shallow rows need the string-aware scan.
fn within_sonic_depth_limit(data: &[u8]) -> bool {
    if memchr::memchr2_iter(b'[', b'{', data).take(32).count() < 32 {
        return true;
    }
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in data {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
        } else {
            match byte {
                b'"' => in_string = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth >= 32 {
                        return false;
                    }
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sonic_numbers() {
        for text in [
            "0",
            "-0",
            "-0.0",
            "1.00",
            "1E+03",
            "1e-03",
            "1e999",
            "165.6263",
            "-0.0003",
            "99999999999999.9999",
            "12345678901234.5678",
            "9223372036854775807",
            "-9223372036854775808",
            "18446744073709551615",
            "18446744073709551616",
            "2.2250738585072014e-308",
            "5e-324",
            "1.7976931348623157e308",
            "9999999999999999999999999999999999999999.999999999999999999999999999999999999",
        ] {
            let serde: serde_json::Value = serde_json::from_str(text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            let expected = JsonValue::as_number(&serde).unwrap();
            let actual = JsonValue::as_number(&sonic).unwrap();
            assert_eq!(actual.as_str(), text);
            assert_eq!(actual.as_i64(), expected.as_i64(), "{text}");
            assert_eq!(actual.as_u64(), expected.as_u64(), "{text}");
            assert_eq!(
                actual.as_f64().map(f64::to_bits),
                expected.as_f64().map(f64::to_bits),
                "{text}"
            );
            assert_eq!(
                actual.string_text().unwrap(),
                expected.string_text().unwrap(),
                "{text}"
            );
        }
    }

    #[test]
    fn test_sonic_fallback() {
        for data in [
            &b""[..],
            b"{",
            b"{} {}",
            b"{} garbage",
            b"[1,]",
            b"{\"a\":01}",
            b"[NaN]",
            b"[Infinity]",
            b"[+1]",
            b"[1.]",
            b"[.1]",
            b"[1e]",
            br#"{"$serde_json::private::Number":"123"}"#,
            br#"{"\u0024serde_json::private::Number":"123"}"#,
            br#"{"a":[{"$serde_json::private::Num\u0062er":"123"}]}"#,
            b"{\"a\":\"\xff\"}",
            br#"{"a":"\ud800"}"#,
        ] {
            assert!(parse_sonic(data).is_none(), "{data:?}");
        }
        let deep = format!("{}0{}", "[".repeat(128), "]".repeat(128));
        assert!(parse_sonic(deep.as_bytes()).is_none());
        assert!(serde_json::from_slice::<serde_json::Value>(deep.as_bytes()).is_err());
        let huge = "[".repeat(100_000);
        assert!(parse_sonic(huge.as_bytes()).is_none());
        let shallow = format!("[{}]", vec!["{}"; 256].join(","));
        assert!(parse_sonic(shallow.as_bytes()).is_some());
        let brackets = serde_json::to_vec(&"{[\\\"".repeat(256)).unwrap();
        assert!(parse_sonic(&brackets).is_some());
        let fast_depth = format!("{}0{}", "[".repeat(31), "]".repeat(31));
        assert!(parse_sonic(fast_depth.as_bytes()).is_some());
        let valid_depth = format!("{}0{}", "[".repeat(127), "]".repeat(127));
        // Valid but deep rows deliberately use serde: Sonic's recursive DOM
        // parser needs substantially more stack per level in debug builds.
        assert!(parse_sonic(valid_depth.as_bytes()).is_none());
        assert!(serde_json::from_slice::<serde_json::Value>(valid_depth.as_bytes()).is_ok());
    }

    #[test]
    fn test_sonic_duplicate_keys_do_not_fall_back() {
        // Neither first-wins nor last-wins is part of COPY's contract for
        // duplicate keys. Do not pay for detecting or normalizing them.
        for text in [r#"{"a":1,"a":2}"#, r#"{"a":1,"\u0061":2}"#] {
            let value = parse_sonic(text.as_bytes()).unwrap();
            let number = JsonValue::as_number(value.object_get("a").unwrap()).unwrap();
            assert!(matches!(number.as_i64(), Some(1 | 2)));
        }
        let nested = parse_sonic(br#"{"a":[{"b":1,"b":2}]}"#).unwrap();
        let value = &JsonValue::as_array(nested.object_get("a").unwrap()).unwrap()[0];
        let number = JsonValue::as_number(value.object_get("b").unwrap()).unwrap();
        assert!(matches!(number.as_i64(), Some(1 | 2)));
        let wide = format!(
            "{{{},\"k0\":2}}",
            (0..16)
                .map(|i| format!("\"k{i}\":1"))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert!(parse_sonic(wide.as_bytes()).is_some());
    }

    #[test]
    fn test_sonic_mutated_input_matches_serde() {
        // Every single-byte mutation, including invalid UTF-8 and control bytes.
        // A fast-path acceptance must also be accepted by the reference parser.
        // Compare serialized DOMs after Serde's duplicate-key normalization;
        // native duplicate-key lookup order is deliberately not compared.
        // Rejections may conservatively fall back.
        for text in [
            r#"{"a":1.23e-4,"b":[true,false,null],"c":"hello\\world"}"#,
            r#"{"a":"汉字\u0061\n\"","b":-0.0003}"#,
            r#"[0,18446744073709551615,99999999999999.9999]"#,
        ] {
            let mut data = text.as_bytes().to_vec();
            for pos in 0..data.len() {
                let original = data[pos];
                for byte in 0..=255 {
                    data[pos] = byte;
                    if let Some(value) = parse_sonic(&data) {
                        let expected: serde_json::Value = serde_json::from_slice(&data)
                            .unwrap_or_else(|e| panic!("Sonic accepted {data:?}: {e}"));
                        let actual: serde_json::Value =
                            serde_json::from_str(&value.to_string()).unwrap();
                        assert_eq!(actual, expected, "{data:?}");
                    }
                }
                data[pos] = original;
            }
        }
    }

    #[test]
    fn test_object_lookup_index_policy() {
        for fields in [4, 16, 64, 256] {
            let text = format!(
                "{{{}}}",
                (0..fields)
                    .map(|i| format!("\"K{i:04}\":{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let serde: serde_json::Value = serde_json::from_str(&text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            for case_sensitive in [false, true] {
                let serde_row = JsonObjectLookup::for_row(&serde, case_sensitive, fields);
                let sonic_row = JsonObjectLookup::for_row(&sonic, case_sensitive, fields);
                let serde_tuple = JsonObjectLookup::for_tuple(&serde, case_sensitive, fields);
                let sonic_tuple = JsonObjectLookup::for_tuple(&sonic, case_sensitive, fields);
                assert_eq!(serde_row.index.is_some(), !case_sensitive);
                assert_eq!(sonic_row.index.is_some(), !case_sensitive || fields >= 16);
                assert_eq!(serde_tuple.index.is_some(), !case_sensitive && fields >= 16);
                assert_eq!(sonic_tuple.index.is_some(), fields >= 16);
                for i in 0..fields {
                    let key = if case_sensitive {
                        format!("K{i:04}")
                    } else {
                        format!("k{i:04}")
                    };
                    for value in [serde_row.get(&key), serde_tuple.get(&key)] {
                        assert_eq!(value.unwrap().as_i64(), Some(i as i64));
                    }
                    for value in [sonic_row.get(&key), sonic_tuple.get(&key)] {
                        assert_eq!(
                            JsonValue::as_number(value.unwrap()).unwrap().as_i64(),
                            Some(i as i64)
                        );
                    }
                }
                assert!(serde_row.get("missing").is_none());
                assert!(sonic_tuple.get("missing").is_none());
                if case_sensitive {
                    // A wide projected source with few lookups should not be indexed.
                    assert!(JsonObjectLookup::for_row(&sonic, true, 4).index.is_none());
                    assert!(JsonObjectLookup::for_tuple(&sonic, true, 4).index.is_none());
                    assert!(sonic_row.get("k0000").is_none());
                    if let Some(index) = &sonic_row.index {
                        assert!(index.keys().all(|key| matches!(key, Cow::Borrowed(_))));
                    }
                }
            }
        }
    }

    #[test]
    fn test_object_lookup_case_fold_collisions() {
        // Distinct JSON keys that fold together are not duplicate input keys.
        // Keep COPY's last-match policy and tuple's first-match policy across
        // both the indexed and small-object scan paths, including Unicode.
        for padding in [0, 32] {
            let mut entries = vec![
                r#""A":1"#.to_string(),
                r#""a":2"#.to_string(),
                r#""Å":3"#.to_string(),
                r#""å":4"#.to_string(),
            ];
            entries.extend((0..padding).map(|i| format!("\"k{i}\":{i}")));
            let text = format!("{{{}}}", entries.join(","));
            let serde: serde_json::Value = serde_json::from_str(&text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            for (key, first, last) in [("a", 1, 2), ("å", 3, 4)] {
                assert_eq!(
                    JsonObjectLookup::for_row(&serde, false, 32)
                        .get(key)
                        .unwrap()
                        .as_i64(),
                    Some(last)
                );
                assert_eq!(
                    JsonObjectLookup::for_tuple(&serde, false, 32)
                        .get(key)
                        .unwrap()
                        .as_i64(),
                    Some(first)
                );
                let row = JsonObjectLookup::for_row(&sonic, false, 32);
                let tuple = JsonObjectLookup::for_tuple(&sonic, false, 32);
                assert_eq!(
                    JsonValue::as_number(row.get(key).unwrap())
                        .unwrap()
                        .as_i64(),
                    Some(last)
                );
                assert_eq!(
                    JsonValue::as_number(tuple.get(key).unwrap())
                        .unwrap()
                        .as_i64(),
                    Some(first)
                );
            }
        }
        for text in ["null", "[]", "1", "\"text\""] {
            let serde: serde_json::Value = serde_json::from_str(text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            assert!(
                JsonObjectLookup::for_row(&serde, false, 32)
                    .get("a")
                    .is_none()
            );
            assert!(
                JsonObjectLookup::for_tuple(&sonic, true, 32)
                    .get("a")
                    .is_none()
            );
        }
    }

    fn assert_sonic_matches_serde(data: &[u8]) {
        if let Some(sonic) = parse_sonic(data) {
            let serde: serde_json::Value = serde_json::from_slice(data)
                .unwrap_or_else(|e| panic!("Sonic accepted invalid JSON {data:?}: {e}"));
            let roundtrip: serde_json::Value = serde_json::from_str(&sonic.to_string()).unwrap();
            assert_eq!(roundtrip, serde, "{data:?}");
            let mut expected = Vec::new();
            let mut actual = Vec::new();
            serde.write_jsonb(&mut expected).unwrap();
            sonic.write_jsonb(&mut actual).unwrap();
            assert_eq!(actual, expected, "{data:?}");
        }
    }

    #[test]
    fn test_sonic_simd_block_boundaries() {
        // Move quotes, escapes, Unicode and numeric tokens across 16/32/64-byte
        // boundaries, and vary the input slice's starting alignment as well.
        for len in 0..=192 {
            let string =
                serde_json::to_string(&format!("{}汉字😀\n\t\"\\\0", "x".repeat(len))).unwrap();
            let text = format!(
                r#"{{"s":{string},"d":99999999999999.9999,"n":18446744073709551616,"a":[5e-324,-0.0]}}"#
            );
            for offset in 0..16 {
                let mut input = vec![b' '; offset];
                input.extend_from_slice(text.as_bytes());
                let data = &input[offset..];
                assert!(parse_sonic(data).is_some(), "len={len}, offset={offset}");
                assert_sonic_matches_serde(data);
            }
        }
    }

    #[test]
    fn test_sonic_truncation_and_invalid_boundaries() {
        for len in [0, 14, 15, 16, 30, 31, 32, 62, 63, 64, 126, 127, 128] {
            let prefix = "x".repeat(len);
            let valid =
                format!(r#"{{"s":"{prefix}\uD83D\uDE00\n\\\"","n":1.23456789012345678E13}}"#);
            assert!(parse_sonic(valid.as_bytes()).is_some());
            for end in 0..=valid.len() {
                assert_sonic_matches_serde(&valid.as_bytes()[..end]);
            }
            for suffix in [r"\uD800", r"\uDC00", r"\uZZZZ", r"\q", "\x01"] {
                let invalid = format!(r#"{{"s":"{prefix}{suffix}"}}"#);
                assert!(serde_json::from_slice::<serde_json::Value>(invalid.as_bytes()).is_err());
                assert!(parse_sonic(invalid.as_bytes()).is_none(), "{invalid:?}");
            }
            let mut invalid_utf8 = format!(r#"{{"s":"{prefix}"#).into_bytes();
            invalid_utf8.extend_from_slice(b"\xff\"}");
            assert!(parse_sonic(&invalid_utf8).is_none());
        }
    }

    #[test]
    fn test_sonic_typed_maps_with_arbitrary_precision() {
        // Cargo unifies the Sonic feature with volo-thrift, which uses typed
        // string maps for biz-extra headers. Its typed contract must not change.
        type Map = std::collections::HashMap<String, String>;
        for text in [
            r#"{}"#,
            r#"{"error":"汉字\n\"","code":"18446744073709551616","zero":"-0"}"#,
        ] {
            let serde: Map = serde_json::from_str(text).unwrap();
            let sonic: Map = sonic_rs::from_str(text).unwrap();
            assert_eq!(sonic, serde);
            let roundtrip: Map =
                serde_json::from_str(&sonic_rs::to_string(&sonic).unwrap()).unwrap();
            assert_eq!(roundtrip, serde);
        }
        for text in [r#"{"n":123}"#, r#"{"n":1e999}"#, r#"{"n":null}"#] {
            assert!(serde_json::from_str::<Map>(text).is_err());
            assert!(sonic_rs::from_str::<Map>(text).is_err());
        }
    }

    fn assert_variant_compatibility(text: &str) {
        let serde: serde_json::Value = serde_json::from_str(text).unwrap();
        let sonic = parse_sonic(text.as_bytes()).unwrap();
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        serde.write_jsonb(&mut expected).unwrap();
        sonic.write_jsonb(&mut actual).unwrap();
        assert_eq!(actual, expected, "{text}");
    }

    #[test]
    fn test_sonic_variant_numbers() {
        for text in [
            "0",
            "-0",
            "0.0",
            "-0.0",
            "1.00",
            "1E+03",
            "1e-03",
            "1e999",
            "-1e999",
            "1e-999",
            "-1e-999",
            "0e9999999999",
            "1e2147483648",
            "1e-2147483649",
            "165.6263",
            "-0.0003",
            "99999999999999.9999",
            "9223372036854775807",
            "9223372036854775808",
            "-9223372036854775808",
            "-9223372036854775809",
            "18446744073709551615",
            "18446744073709551616",
            "99999999999999999999999999999999999999",
            "100000000000000000000000000000000000000",
            "-99999999999999999999999999999999999999",
            "-100000000000000000000000000000000000000",
            "170141183460469231731687303715884105727",
            "170141183460469231731687303715884105728",
            "-170141183460469231731687303715884105728",
            "-170141183460469231731687303715884105729",
            "340282366920938463463374607431768211455",
            "340282366920938463463374607431768211456",
            "2.2250738585072014e-308",
            "5e-324",
            "1.7976931348623157e308",
        ] {
            assert_variant_compatibility(text);
            assert_variant_compatibility(&format!(r#"{{"n":{text},"a":[{text}]}}"#));
        }
    }

    #[test]
    fn test_sonic_variant_borrows_strings() {
        let sonic = parse_sonic(br#"{"payload":["hello","escaped\ntext"]}"#).unwrap();
        let jsonb::Value::Object(object) = sonic_to_jsonb(&sonic) else {
            unreachable!()
        };
        let jsonb::Value::Array(array) = object.get("payload").unwrap() else {
            unreachable!()
        };
        assert!(matches!(
            &array[0],
            jsonb::Value::String(Cow::Borrowed("hello"))
        ));
        assert!(matches!(
            &array[1],
            jsonb::Value::String(Cow::Borrowed("escaped\ntext"))
        ));
    }

    #[test]
    fn test_sonic_variant_generated_compatibility() {
        // Exercise unique-key nested trees with changing numbers and escapes.
        let mut state = 0x1234_5678_9abc_def0u64;
        for i in 0..1024 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let integer = (state as u128) << 64 | state.rotate_left(17) as u128;
            let text = format!(
                r#"{{"id":{i},"integer":{integer},"fraction":{state}.1234,"exponent":{state}e-{},"a":[null,true,false,{{"s":"汉字\\\"\n\u0000","n":-{integer}}}]}}"#,
                i % 400
            );
            assert_variant_compatibility(&text);
        }
    }

    #[test]
    fn test_sonic_variant_compatibility() {
        for text in [
            r#"{"Å":"汉字\n\"\\","arr":[null,true,false,-0.0,1E3]}"#,
            r#"{"n":99999999999999.9999,"d":-0.0003}"#,
            r#"{"A":1,"a":2,"empty":{},"array":[]}"#,
        ] {
            let serde: serde_json::Value = serde_json::from_str(text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            let mut expected = Vec::new();
            let mut actual = Vec::new();
            serde.write_jsonb(&mut expected).unwrap();
            sonic.write_jsonb(&mut actual).unwrap();
            assert_eq!(actual, expected, "{text}");
            assert_eq!(sonic.diagnostic_text(), serde.to_string(), "{text}");
            assert_eq!(
                JsonValue::object_iter(&sonic).unwrap().len(),
                serde.as_object().unwrap().len()
            );
        }
    }
}
