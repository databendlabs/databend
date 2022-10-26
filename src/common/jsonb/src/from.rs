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

use decimal_rs::Decimal;

use super::value::Object;
use super::value::Value;

macro_rules! from_integer {
    ($($ty:ident)*) => {
        $(
            impl<'a> From<$ty> for Value<'a> {
                fn from(n: $ty) -> Self {
                    Value::Number(n.into())
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
                    Value::Number(n.try_into().unwrap())
                }
            }
        )*
    };
}

from_integer! {
    i8 i16 i32 i64 isize
    u8 u16 u32 u64 usize
}

from_float! {
    f32 f64
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

impl<'a> From<Decimal> for Value<'a> {
    fn from(d: Decimal) -> Self {
        Value::Number(d)
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
