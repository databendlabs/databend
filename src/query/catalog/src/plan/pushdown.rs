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

use common_expression::types::DataType;
use common_expression::RemoteExpr;
use common_expression::TableField;
use common_expression::TableSchema;

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
    pub filter: RemoteExpr<String>,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct PushDownInfo {
    /// Optional column indices to use as a projection
    pub projection: Option<Projection>,
    /// Optional filter expression plan
    /// split_conjunctions by `and` operator
    pub filters: Vec<RemoteExpr<String>>,
    /// Optional prewhere information
    /// used for prewhere optimization
    pub prewhere: Option<PrewhereInfo>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
    /// Optional order_by expression plan, asc, null_first
    pub order_by: Vec<(RemoteExpr<String>, bool, bool)>,
}

/// TopK is a wrapper for topk push down items.
/// We only take the first column in order_by as the topk column.
#[derive(Debug, Clone)]
pub struct TopK {
    pub limit: usize,
    pub order_by: TableField,
    pub asc: bool,
    pub column_id: u32,
}

impl PushDownInfo {
    pub fn top_k(&self, schema: &TableSchema, support: fn(&DataType) -> bool) -> Option<TopK> {
        if !self.order_by.is_empty() && self.limit.is_some() {
            let order = &self.order_by[0];
            let limit = self.limit.unwrap();

            const MAX_TOPK_LIMIT: usize = 1000;
            if limit > MAX_TOPK_LIMIT {
                return None;
            }

            if let RemoteExpr::<String>::ColumnRef { id, .. } = &order.0 {
                // TODO: support sub column of nested type.
                let field = schema.field_with_name(id).unwrap();
                let data_type: DataType = field.data_type().into();
                if !support(&data_type) {
                    return None;
                }

                let (leaf_column_ids, leaf_fields) = schema.leaf_fields();
                let index = leaf_fields.iter().position(|p| p == field).unwrap();
                let column_id = leaf_column_ids[index];

                let top_k = TopK {
                    limit: self.limit.unwrap(),
                    order_by: field.clone(),
                    asc: order.1,
                    column_id,
                };
                Some(top_k)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn prewhere_of_push_downs(push_downs: &Option<PushDownInfo>) -> Option<PrewhereInfo> {
        if let Some(PushDownInfo { prewhere, .. }) = push_downs {
            prewhere.clone()
        } else {
            None
        }
    }

    pub fn projection_of_push_downs(
        schema: &TableSchema,
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
