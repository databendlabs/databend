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
use std::hash::Hash;
use std::ops::Deref;

use common_exception::ErrorCode;
use common_exception::Result;
use itertools::EitherOrBoth::Both;
use itertools::EitherOrBoth::Left;
use itertools::EitherOrBoth::Right;
use itertools::Itertools;
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
    // calculate memory size of JSON value
    pub fn calculate_memory_size(&self) -> usize {
        let mut memory_size = 0;
        let mut values = Vec::new();
        let value = self.as_ref();
        values.push(value);
        while !values.is_empty() {
            memory_size += std::mem::size_of::<Value>();
            let value = values.pop().unwrap();
            match value {
                Value::String(val) => {
                    memory_size += val.len();
                }
                Value::Array(vals) => {
                    for val in vals.iter() {
                        values.push(val);
                    }
                }
                Value::Object(obj) => {
                    for (key, val) in obj.iter() {
                        memory_size += key.len();
                        values.push(val);
                    }
                }
                _ => {}
            }
        }
        memory_size
    }

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
                let it = a1.iter().zip_longest(a2.iter()).find_map(|e| match e {
                    Both(v1, v2) => {
                        match VariantValue::from(v1).partial_cmp(&VariantValue::from(v2)) {
                            Some(ord) => match ord {
                                Ordering::Equal => None,
                                other => Some(other),
                            },
                            None => None,
                        }
                    }
                    Left(_) => Some(Ordering::Greater),
                    Right(_) => Some(Ordering::Less),
                });
                match it {
                    Some(ord) => ord,
                    None => Ordering::Equal,
                }
            }
            (Value::Object(o1), Value::Object(o2)) => {
                let it = o1.keys().zip_longest(o2.keys()).find_map(|e| match e {
                    Both(k1, k2) => match k1.cmp(k2) {
                        Ordering::Equal => {
                            let v1 = o1.get(k1).unwrap();
                            let v2 = o2.get(k2).unwrap();
                            match VariantValue::from(v1).cmp(&VariantValue::from(v2)) {
                                Ordering::Equal => None,
                                other => Some(other),
                            }
                        }
                        Ordering::Greater => Some(Ordering::Less),
                        Ordering::Less => Some(Ordering::Greater),
                    },
                    Left(_) => Some(Ordering::Greater),
                    Right(_) => Some(Ordering::Less),
                });
                match it {
                    Some(ord) => ord,
                    None => Ordering::Equal,
                }
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

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for VariantValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let v = self.as_ref().to_string();
        let u = v.as_bytes();
        Hash::hash(&u, state);
    }
}

impl Display for VariantValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
