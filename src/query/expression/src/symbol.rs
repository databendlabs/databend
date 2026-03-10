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

use std::fmt::Debug;
use std::fmt::Write;
use std::hash::Hash;

use num_traits::FromPrimitive;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, num_derive::FromPrimitive)]
#[repr(usize)]
pub enum DummyColumnType {
    Default = 0,
    WindowFunction = 1,
    AggregateFunction = 2,
    Subquery = 3,
    UDF = 4,
    AsyncFunction = 5,
    Other = 6,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct Symbol(usize);

impl Symbol {
    const MIN_DUMMY_COLUMN: Self = Self::new_dummy_column(DummyColumnType::Other);

    pub const DUMMY_COLUMN: Self = Self::new_dummy_column(DummyColumnType::Default);

    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn new_dummy_column(dummy_type: DummyColumnType) -> Self {
        Self(usize::MAX - dummy_type as usize)
    }

    pub const fn as_usize(&self) -> usize {
        self.0
    }

    pub const fn is_dummy_column(&self) -> bool {
        self.0 >= Self::MIN_DUMMY_COLUMN.0
    }

    pub fn dummy_column_type(&self) -> Option<DummyColumnType> {
        if !self.is_dummy_column() {
            return None;
        }

        DummyColumnType::from_usize(usize::MAX - self.0)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Symbol {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<usize>().map(Symbol)
    }
}

impl std::fmt::Debug for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        usize::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum SymbolOrOffset {
    Symbol(Symbol),
    Offset(usize),
}

impl SymbolOrOffset {
    pub const fn as_symbol(&self) -> Option<Symbol> {
        match *self {
            SymbolOrOffset::Symbol(index) => Some(index),
            SymbolOrOffset::Offset(_) => None,
        }
    }
}
