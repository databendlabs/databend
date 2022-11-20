// Copyright 2022 Datafuse Labs.
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

use core::iter::FromIterator;
use std::borrow::Cow;

use ordered_float::OrderedFloat;
use serde_json::Map as JsonMap;
use serde_json::Number as JsonNumber;
use serde_json::Value as JsonValue;

use super::number::Number;
use super::value::Object;
use super::value::Value;

macro_rules! from_signed_integer {
    ($($ty:ident)*) => {
        $(
            impl<'a> From<$ty> for Value<'a> {
                fn from(n: $ty) -> Self {
                    Value::Number(Number::Int64(n as i64))
                }
            }
        )*
    };
}

macro_rules! from_unsigned_integer {
    ($($ty:ident)*) => {
        $(
            impl<'a> From<$ty> for Value<'a> {
                fn from(n: $ty) -> Self {
                    Value::Number(Number::UInt64(n as u64))
                }
            }
        )*
    };
}

macro_rules! from_float {
    ($($ty:ident)*) => {
        $(
            impl<'a> From<$ty> for Value<'a> {
                fn from(n: $ty) -> Self {
                    Value::Number(Number::Float64(n as f64))
                }
            }
        )*
    };
}

from_signed_integer! {
    i8 i16 i32 i64 isize
}

from_unsigned_integer! {
    u8 u16 u32 u64 usize
}

from_float! {
    f32 f64
}

impl<'a> From<OrderedFloat<f32>> for Value<'a> {
    fn from(f: OrderedFloat<f32>) -> Self {
        Value::Number(Number::Float64(f.0 as f64))
    }
}

impl<'a> From<OrderedFloat<f64>> for Value<'a> {
    fn from(f: OrderedFloat<f64>) -> Self {
        Value::Number(Number::Float64(f.0))
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(f: bool) -> Self {
        Value::Bool(f)
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(f: String) -> Self {
        Value::String(f.into())
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(f: &'a str) -> Self {
        Value::String(Cow::from(f))
    }
}

impl<'a> From<Cow<'a, str>> for Value<'a> {
    fn from(f: Cow<'a, str>) -> Self {
        Value::String(f)
    }
}

impl<'a> From<Object<'a>> for Value<'a> {
    fn from(o: Object<'a>) -> Self {
        Value::Object(o)
    }
}

impl<'a, T: Into<Value<'a>>> From<Vec<T>> for Value<'a> {
    fn from(f: Vec<T>) -> Self {
        Value::Array(f.into_iter().map(Into::into).collect())
    }
}

impl<'a, T: Clone + Into<Value<'a>>> From<&'a [T]> for Value<'a> {
    fn from(f: &'a [T]) -> Self {
        Value::Array(f.iter().cloned().map(Into::into).collect())
    }
}

impl<'a, T: Into<Value<'a>>> FromIterator<T> for Value<'a> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Value::Array(iter.into_iter().map(Into::into).collect())
    }
}

impl<'a, K: Into<String>, V: Into<Value<'a>>> FromIterator<(K, V)> for Value<'a> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Value::Object(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<'a> From<()> for Value<'a> {
    fn from((): ()) -> Self {
        Value::Null
    }
}

impl<'a> From<&JsonValue> for Value<'a> {
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(v) => Value::Bool(*v),
            JsonValue::Number(v) => {
                if v.is_u64() {
                    Value::Number(Number::UInt64(v.as_u64().unwrap()))
                } else if v.is_i64() {
                    Value::Number(Number::Int64(v.as_i64().unwrap()))
                } else {
                    Value::Number(Number::Float64(v.as_f64().unwrap()))
                }
            }
            JsonValue::String(v) => Value::String(v.clone().into()),
            JsonValue::Array(arr) => {
                let mut vals: Vec<Value> = Vec::with_capacity(arr.len());
                for val in arr {
                    vals.push(val.into());
                }
                Value::Array(vals)
            }
            JsonValue::Object(obj) => {
                let mut map = Object::new();
                for (k, v) in obj.iter() {
                    let val: Value = v.into();
                    map.insert(k.to_string(), val);
                }
                Value::Object(map)
            }
        }
    }
}

impl<'a> From<Value<'a>> for JsonValue {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::Null => JsonValue::Null,
            Value::Bool(v) => JsonValue::Bool(v),
            Value::Number(v) => match v {
                Number::Int64(v) => JsonValue::Number(v.into()),
                Number::UInt64(v) => JsonValue::Number(v.into()),
                Number::Float64(v) => JsonValue::Number(JsonNumber::from_f64(v).unwrap()),
            },
            Value::String(v) => JsonValue::String(v.to_string()),
            Value::Array(arr) => {
                let mut vals: Vec<JsonValue> = Vec::with_capacity(arr.len());
                for val in arr {
                    vals.push(val.into());
                }
                JsonValue::Array(vals)
            }
            Value::Object(obj) => {
                let mut map = JsonMap::new();
                for (k, v) in obj.iter() {
                    let val: JsonValue = v.clone().into();
                    map.insert(k.to_string(), val);
                }
                JsonValue::Object(map)
            }
        }
    }
}
