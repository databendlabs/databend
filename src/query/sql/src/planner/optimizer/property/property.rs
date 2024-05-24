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

use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;

use super::column_stat::ColumnStatSet;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SortItem;
use crate::IndexType;

pub type ColumnSet = HashSet<IndexType>;
pub type TableSet = HashSet<IndexType>;

#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequiredProperty {
    pub distribution: Distribution,
}

impl RequiredProperty {
    pub fn satisfied_by(&self, physical: &PhysicalProperty) -> bool {
        self.distribution.satisfied_by(&physical.distribution)
    }
}

impl Display for RequiredProperty {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{{ dist: {} }}", self.distribution)
    }
}

#[derive(Default, Clone, Debug)]
pub struct Statistics {
    // We can get the precise row count of a table in databend,
    // which information is useful to optimize some queries like `COUNT(*)`.
    pub precise_cardinality: Option<u64>,
    /// Statistics of columns, column index -> column stat
    pub column_stats: ColumnStatSet,
}

#[derive(Default, Clone, Debug)]
pub struct StatInfo {
    // TODO(leiysky): introduce upper bound of cardinality to
    // reduce error in estimation.
    pub cardinality: f64,
    pub statistics: Statistics,
}

#[derive(Default, Clone, Debug)]
pub struct RelationalProperty {
    /// Output columns of a relational expression
    pub output_columns: ColumnSet,

    /// Outer references of a relational expression
    pub outer_columns: ColumnSet,

    /// Used columns of a relational expression
    pub used_columns: ColumnSet,

    /// Ordering information of a relational expression
    /// The sequence of sort items is important.
    /// No ordering information is ensured if empty.
    ///
    /// TODO(leiysky): it's better to place ordering property
    /// to the physical property, but at that time, we will have
    /// to enforce the ordering property manually.
    pub orderings: Vec<SortItem>,

    /// only sort in partition level
    /// used in window sort after shuffle
    pub partition_orderings: Option<(Vec<ScalarItem>, Vec<SortItem>)>,
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalProperty {
    pub distribution: Distribution,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Distribution {
    Any,
    Random,
    Serial,
    Broadcast,
    Hash(Vec<ScalarExpr>),
}

impl Default for Distribution {
    // Only used for `RequiredProperty`
    fn default() -> Self {
        Self::Any
    }
}

impl Distribution {
    /// Check if required distribution is satisfied by given distribution.
    pub fn satisfied_by(&self, distribution: &Distribution) -> bool {
        // (required, delivered)
        match (&self, distribution) {
            (Distribution::Any, _)
            | (Distribution::Random, _)
            | (Distribution::Serial, Distribution::Serial)
            | (Distribution::Broadcast, Distribution::Broadcast)
            | (Distribution::Hash(_), Distribution::Broadcast) => true,

            (Distribution::Hash(ref keys), Distribution::Hash(ref other_keys)) => {
                keys == other_keys
            }
            _ => false,
        }
    }
}

impl Display for Distribution {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Distribution::Any => write!(f, "Any"),
            Distribution::Random => write!(f, "Random"),
            Distribution::Serial => write!(f, "Serial"),
            Distribution::Broadcast => write!(f, "Broadcast"),
            Distribution::Hash(ref keys) => write!(
                f,
                "Hash({})",
                keys.iter()
                    .map(|s| s.as_raw_expr().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}
