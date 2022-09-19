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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::{self};
use std::io::Write;
use std::ops::Neg;

use decimal_rs::Decimal;

use super::error::Error;
use super::ser::Encoder;

pub type Object<'a> = BTreeMap<String, Value<'a>>;

// JSONB value
#[derive(Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    String(Cow<'a, str>),
    Number(Decimal),
    Array(Vec<Value<'a>>),
    Object(Object<'a>),
}

impl<'a> Debug for Value<'a> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Null => formatter.debug_tuple("Null").finish(),
            Value::Bool(v) => formatter.debug_tuple("Bool").field(&v).finish(),
            Value::Number(ref v) => Debug::fmt(v, formatter),
            Value::String(ref v) => formatter.debug_tuple("String").field(v).finish(),
            Value::Array(ref v) => {
                formatter.write_str("Array(")?;
                Debug::fmt(v, formatter)?;
                formatter.write_str(")")
            }
            Value::Object(ref v) => {
                formatter.write_str("Object(")?;
                Debug::fmt(v, formatter)?;
                formatter.write_str(")")
            }
        }
    }
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(v) => {
                if *v {
                    write!(f, "true")
                } else {
                    write!(f, "false")
                }
            }
            Value::Number(ref v) => write!(f, "{}", v),
            Value::String(ref v) => {
                write!(f, "{:?}", v)
            }
            Value::Array(ref vs) => {
                let mut first = true;
                write!(f, "[")?;
                for v in vs.iter() {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Object(ref vs) => {
                let mut first = true;
                write!(f, "{{")?;
                for (k, v) in vs.iter() {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{:?}", k)?;
                    write!(f, ":")?;
                    write!(f, "{v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl<'a> Value<'a> {
    pub fn is_object(&self) -> bool {
        self.as_object().is_some()
    }

    pub fn as_object(&self) -> Option<&Object<'a>> {
        match self {
            Value::Object(ref obj) => Some(obj),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn as_array(&self) -> Option<&Vec<Value<'a>>> {
        match self {
            Value::Array(ref array) => Some(array),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn as_str(&self) -> Option<&Cow<'_, str>> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_number(&self) -> bool {
        matches!(self, Value::Number(_))
    }

    pub fn is_i64(&self) -> bool {
        self.as_i64().is_some()
    }

    pub fn is_u64(&self) -> bool {
        self.as_u64().is_some()
    }

    pub fn is_f64(&self) -> bool {
        self.as_f64().is_some()
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Number(d) => {
                if d.scale() == 0 {
                    let (v, _, is_neg) = d.into_parts();
                    let v = v as i64;
                    if is_neg { Some(v.neg()) } else { Some(v) }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Number(d) => {
                if d.scale() == 0 && d.is_sign_positive() {
                    let (v, _, is_neg) = d.into_parts();
                    if !is_neg { Some(v as u64) } else { None }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(d) => Some(d.into()),
            _ => None,
        }
    }

    pub fn is_boolean(&self) -> bool {
        self.as_bool().is_some()
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        self.as_null().is_some()
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Value::Null => Some(()),
            _ => None,
        }
    }

    /// Attempts to serialize the JSONB Value into a byte stream.
    pub fn to_vec(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let mut encoder = Encoder::new(buf);
        encoder.encode(self)?;
        Ok(())
    }
}
