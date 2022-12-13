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

use std::fmt::Debug;

use common_datavalues::DataSchemaRef;

use crate::plan::Expression;
use crate::plan::Projection;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrewhereInfo {
    /// columns to be output by prewhere scan
    pub output_columns: Projection,
    /// columns used for prewhere
    pub prewhere_columns: Projection,
    /// remain_columns = scan.columns - need_columns
    pub remain_columns: Projection,
    /// filter for prewhere
    pub filter: Expression,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct PushDownInfo {
    /// Optional column indices to use as a projection
    pub projection: Option<Projection>,
    /// Optional filter expression plan
    /// split_conjunctions by `and` operator
    pub filters: Vec<Expression>,
    /// Optional prewhere information
    /// used for prewhere optimization
    pub prewhere: Option<PrewhereInfo>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
    /// Optional order_by expression plan, asc, null_first
    pub order_by: Vec<(Expression, bool, bool)>,
}

impl PushDownInfo {
    pub fn prewhere_of_push_downs(push_downs: &Option<PushDownInfo>) -> Option<PrewhereInfo> {
        if let Some(PushDownInfo { prewhere, .. }) = push_downs {
            prewhere.clone()
        } else {
            None
        }
    }

    pub fn projection_of_push_downs(
        schema: &DataSchemaRef,
        push_downs: &Option<PushDownInfo>,
    ) -> Projection {
        if let Some(PushDownInfo {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            let indices = (0..schema.fields().len())
                .into_iter()
                .collect::<Vec<usize>>();
            Projection::Columns(indices)
        }
    }
}
