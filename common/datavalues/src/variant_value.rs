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

use core::str::FromStr;
use std::cmp::Ordering;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct VariantValue(pub Value);

impl From<Value> for VariantValue {
    fn from(val: Value) -> Self {
        VariantValue(val)
    }
}

impl From<&Value> for VariantValue {
    fn from(val: &Value) -> Self {
        VariantValue(val.clone())
    }
}

impl AsRef<Value> for VariantValue {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl Deref for VariantValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for VariantValue {
    fn default() -> Self {
        VariantValue::from(Value::Null)
    }
}

impl FromStr for VariantValue {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self> {
        match serde_json::Value::from_str(s) {
            Ok(v) => Ok(VariantValue::from(v)),
            Err(_) => Err(ErrorCode::StrParseError(format!(
                "Unknown VariantValue: {}",
                s
            ))),
        }
    }
}

impl VariantValue {
    fn level(&self) -> u8 {
        match self.as_ref() {
            Value::Null => 0,
            Value::Array(_) => 1,
            Value::Object(_) => 2,
            Value::String(_) => 3,
            Value::Number(_) => 4,
            Value::Bool(_) => 5,
        }
    }
}

// VariantValue compares as the following rule:
// Null > Array > Object > String > Number > Boolean
// The Array compares each element in turn.
// The Object compares each key-value in turn,
// first compare the key, and then compare the value if the key is equal.
// The Greater the key, the Less the Object, the Greater the value, the Greater the Object
impl Ord for VariantValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let l1 = self.level();
        let l2 = other.level();
        if l1 != l2 {
            return l1.cmp(&l2).reverse();
        }
        match (self.as_ref(), other.as_ref()) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Array(a1), Value::Array(a2)) => {
                for (v1, v2) in a1.iter().zip(a2) {
                    if !v1.eq(v2) {
                        return VariantValue::from(v1).cmp(&VariantValue::from(v2));
                    }
                }
                a1.len().cmp(&a2.len())
            }
            (Value::Object(o1), Value::Object(o2)) => {
                for (k1, k2) in o1.keys().zip(o2.keys()) {
                    if k1.eq(k2) {
                        let v1 = o1.get(k1).unwrap();
                        let v2 = o2.get(k2).unwrap();

                        let res = VariantValue::from(v1).cmp(&VariantValue::from(v2));
                        if res == Ordering::Equal {
                            continue;
                        }
                        return res;
                    }
                    return k1.cmp(k2).reverse();
                }
                o1.len().cmp(&o2.len())
            }
            (Value::String(v1), Value::String(v2)) => v1.cmp(v2),
            (Value::Number(v1), Value::Number(v2)) => {
                if v1.is_f64() || v2.is_f64() {
                    let n1 = if v1.is_u64() {
                        let n1 = v1.as_u64().unwrap();
                        n1 as f64
                    } else if v1.is_i64() {
                        let n1 = v1.as_i64().unwrap();
                        n1 as f64
                    } else {
                        v1.as_f64().unwrap()
                    };
                    let n2 = if v2.is_u64() {
                        let n2 = v2.as_u64().unwrap();
                        n2 as f64
                    } else if v2.is_i64() {
                        let n2 = v2.as_i64().unwrap();
                        n2 as f64
                    } else {
                        v2.as_f64().unwrap()
                    };
                    if n1 > n2 {
                        Ordering::Greater
                    } else if n1 < n2 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                } else if v1.is_u64() && v2.is_u64() {
                    let n1 = v1.as_u64().unwrap();
                    let n2 = v2.as_u64().unwrap();
                    n1.cmp(&n2)
                } else if v1.is_i64() && v2.is_i64() {
                    let n1 = v1.as_i64().unwrap();
                    let n2 = v2.as_i64().unwrap();
                    n1.cmp(&n2)
                } else if v1.is_u64() && v2.is_i64() {
                    let n1 = v1.as_u64().unwrap();
                    let n2 = v2.as_i64().unwrap();
                    if n2 < 0 || n1 > 9223372036854775807 {
                        return Ordering::Greater;
                    }
                    let n2 = n2 as u64;
                    n1.cmp(&n2)
                } else if v1.is_i64() && v2.is_u64() {
                    let n1 = v1.as_i64().unwrap();
                    let n2 = v2.as_u64().unwrap();
                    if n1 < 0 || n2 > 9223372036854775807 {
                        return Ordering::Less;
                    }
                    let n1 = n1 as u64;
                    n1.cmp(&n2)
                } else {
                    Ordering::Equal
                }
            }
            (Value::Bool(v1), Value::Bool(v2)) => {
                if *v1 && !*v2 {
                    return Ordering::Greater;
                } else if !*v1 && *v2 {
                    return Ordering::Less;
                }
                Ordering::Equal
            }
            (_, _) => Ordering::Equal,
        }
    }
}

impl PartialOrd for VariantValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for VariantValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
