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

use std::fmt;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct IntervalType {
    kind: IntervalKind,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalKind {
    Year,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Doy,
    Dow,
}

impl fmt::Display for IntervalKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            IntervalKind::Year => "YEAR",
            IntervalKind::Quarter => "QUARTER",
            IntervalKind::Month => "MONTH",
            IntervalKind::Day => "DAY",
            IntervalKind::Hour => "HOUR",
            IntervalKind::Minute => "MINUTE",
            IntervalKind::Second => "SECOND",
            IntervalKind::Doy => "DOY",
            IntervalKind::Dow => "DOW",
        })
    }
}

impl From<String> for IntervalKind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "YEAR" => IntervalKind::Year,
            "QUARTER" => IntervalKind::Quarter,
            "MONTH" => IntervalKind::Month,
            "DAY" => IntervalKind::Day,
            "HOUR" => IntervalKind::Hour,
            "MINUTE" => IntervalKind::Minute,
            "SECOND" => IntervalKind::Second,
            "DOY" => IntervalKind::Doy,
            "DOW" => IntervalKind::Dow,
            _ => unreachable!(),
        }
    }
}

impl IntervalType {
    pub fn new(kind: IntervalKind) -> Self {
        Self { kind }
    }

    pub fn new_impl(kind: IntervalKind) -> DataTypeImpl {
        DataTypeImpl::Interval(Self { kind })
    }

    pub fn kind(&self) -> &IntervalKind {
        &self.kind
    }
}

impl DataType for IntervalType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Interval
    }

    fn name(&self) -> String {
        format!("Interval({})", self.kind)
    }
}

impl std::fmt::Debug for IntervalType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}({:?})", self.name(), self.kind)
    }
}
