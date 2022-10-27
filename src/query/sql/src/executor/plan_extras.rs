// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use common_datavalues::DataSchema;

use crate::plans::Projection;

use common_planner::PhysicalScalar;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct PrewhereInfo {
    /// columns to be ouput be prewhere scan
    pub output_columns: Projection,
    /// columns used for prewhere
    pub prewhere_columns: Projection,
    /// remain_columns = scan.columns - need_columns
    pub remain_columns: Projection,
    /// filter for prewhere
    pub filter: PhysicalScalar,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Extras {
    /// Optional column indices to use as a projection
    pub projection: Option<Projection>,
    /// Optional filter expression plan
    /// split_conjunctions by `and` operator
    pub filters: Vec<PhysicalScalar>,
    /// Optional prewhere information
    /// used for prewhere optimization
    pub prewhere: Option<PrewhereInfo>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
    /// Optional order_by expression plan,
    ///  expression: PhysicalScalar, asc: bool, nulls_first
    pub order_by: Vec<(PhysicalScalar, bool, bool)>,
}

impl Extras {
    pub fn default() -> Self {
        Extras {
            projection: None,
            filters: vec![],
            prewhere: None,
            limit: None,
            order_by: vec![],
        }
    }
}



#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StageKind {
    Normal,
    Expansive,
    Merge,
}
