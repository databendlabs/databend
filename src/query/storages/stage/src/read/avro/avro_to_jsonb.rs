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

use std::collections::BTreeMap;

use apache_avro::types::Value;

pub(super) fn to_jsonb(value: &Value) -> Result<jsonb::Value, String> {
    let jvalue = match value {
        Value::Null => jsonb::Value::Null,
        Value::Boolean(v) => jsonb::Value::Bool(*v),
        Value::Int(v) => jsonb::Value::from(*v),
        Value::Long(v) => jsonb::Value::from(*v),
        Value::Float(v) => jsonb::Value::from(*v),
        Value::Double(v) => jsonb::Value::from(*v),
        Value::String(v) => jsonb::Value::from(v.as_str()),
        Value::Enum(_, v) => jsonb::Value::from(v.as_str()),
        Value::Union(_, v) => to_jsonb(v)?,
        Value::Array(v) => {
            let mut array = Vec::with_capacity(v.len());
            for v in v {
                array.push(to_jsonb(v)?)
            }
            jsonb::Value::Array(array)
        }
        Value::Map(v) => {
            let mut array = Vec::with_capacity(v.len());
            for (k, v) in v {
                array.push((k.clone(), to_jsonb(v)?));
            }
            jsonb::Value::Object(BTreeMap::from_iter(array))
        }
        Value::Record(v) => {
            let mut array = Vec::with_capacity(v.len());
            for (k, v) in v {
                array.push((k.clone(), to_jsonb(v)?));
            }
            jsonb::Value::Object(BTreeMap::from_iter(array))
        }
        _ => return Err(format!("Cannot convert {:?} to JSONB", value)),
        // Value::Bytes(_v) | Value::Fixed(_, _v) => {}
        // Value::Date(_) => {}
        // Value::Decimal(_) => {}
        // Value::BigDecimal(_) => {}
        // Value::TimeMillis(_) => {}
        // Value::TimeMicros(_) => {}
        // Value::TimestampMillis(_) => {}
        // Value::TimestampMicros(_) => {}
        // Value::TimestampNanos(_) => {}
        // Value::LocalTimestampMillis(_) => {}
        // Value::LocalTimestampMicros(_) => {}
        // Value::LocalTimestampNanos(_) => {}
        // Value::Duration(_) => {}
        // Value::Uuid(_) => {}
    };
    Ok(jvalue)
}
