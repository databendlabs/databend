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

use apache_avro::types::Value;

pub(super) fn to_jsonb(value: &Value) -> jsonb::Value {
    match value {
        Value::Null => jsonb::Value::Null,
        Value::Boolean(v) => jsonb::Value::Bool(*v),
        Value::Int(v) => jsonb::Value::from(*v),
        Value::Long(v) => jsonb::Value::from(*v),
        Value::Float(v) => jsonb::Value::from(*v),
        Value::Double(v) => jsonb::Value::from(*v),
        Value::String(v) => jsonb::Value::from(v.as_str()),
        Value::Enum(_, v) => jsonb::Value::from(v.as_str()),
        Value::Union(_, v) => to_jsonb(v),
        Value::Array(v) => jsonb::Value::Array(v.as_slice().iter().map(to_jsonb).collect()),
        Value::Map(v) => {
            jsonb::Value::Object(v.iter().map(|(k, v)| (k.clone(), to_jsonb(v))).collect())
        }
        Value::Record(v) => {
            jsonb::Value::Object(v.iter().map(|(k, v)| (k.clone(), to_jsonb(v))).collect())
        }
        Value::Bytes(_v) | Value::Fixed(_, _v) => {
            todo!()
        }
        _ => jsonb::Value::Array(vec![]),
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
    }
}
