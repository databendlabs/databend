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

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use itertools::EitherOrBoth::Both;
use itertools::EitherOrBoth::Left;
use itertools::EitherOrBoth::Right;
use itertools::Itertools;

use crate::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, PartialEq)]
pub struct StructValue {
    pub values: Vec<DataValue>,
}

impl Eq for StructValue {}

impl StructValue {
    pub fn new(values: Vec<DataValue>) -> Self {
        Self { values }
    }

    pub fn inner_types(&self) -> Option<Vec<DataTypeImpl>> {
        let mut types = Vec::with_capacity(self.values.len());
        for value in &self.values {
            types.push(value.max_data_type());
        }
        if !types.is_empty() {
            return Some(types);
        }
        None
    }
}

impl From<DataValue> for StructValue {
    fn from(val: DataValue) -> Self {
        match val {
            DataValue::Struct(v) => StructValue::new(v),
            _ => StructValue::new(vec![val]),
        }
    }
}

impl PartialOrd for StructValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for StructValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Display for StructValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.values)
    }
}

#[derive(Copy, Clone)]
pub enum StructValueRef<'a> {
    Indexed {
        column: &'a StructColumn,
        idx: usize,
    },
    ValueRef {
        val: &'a StructValue,
    },
}

impl<'a> StructValueRef<'a> {
    pub fn values(&self) -> Vec<DataValue> {
        match self {
            StructValueRef::Indexed { column, idx } => {
                let value = column.get(*idx);
                value.as_struct().unwrap()
            }
            StructValueRef::ValueRef { val } => val.values.clone(),
        }
    }
}

impl PartialEq for StructValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.values().eq(&other.values())
    }
}

impl PartialOrd for StructValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.values();
        let r = other.values();
        let it = l.iter().zip_longest(r.iter()).find_map(|e| match e {
            Both(ls, rs) => match ls.partial_cmp(rs) {
                Some(ord) => match ord {
                    Ordering::Equal => None,
                    other => Some(other),
                },
                None => None,
            },
            Left(_) => Some(Ordering::Greater),
            Right(_) => Some(Ordering::Less),
        });
        it.or(Some(Ordering::Equal))
    }
}

impl Eq for StructValueRef<'_> {}

impl Ord for StructValueRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Debug for StructValueRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StructValueRef::Indexed { column, idx } => {
                let value = column.get(*idx);
                if let DataValue::Struct(vals) = value {
                    for val in vals {
                        write!(f, "{:?}", val)?;
                    }
                }
                Ok(())
            }
            StructValueRef::ValueRef { val } => write!(f, "{:?}", val),
        }
    }
}
