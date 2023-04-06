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
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;

use crate::plan::Projection;

/// Information of Virtual Columns.
///
/// Generated from the source column by the paths.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VirtualColumnInfo {
    /// Source column name
    pub source_name: String,
    /// Virtual column name
    pub name: String,
    /// Paths to generate virtual column from source column
    pub paths: Vec<Scalar>,
    /// Virtual column data type
    pub data_type: Box<TableDataType>,
}

/// Information about prewhere optimization.
///
/// Prewhere steps:
///
/// 1. Read columns by `prewhere_columns`.
/// 2. Filter data by `filter`.
/// 3. Read columns by `remain_columns`.
/// 4. If virtual columns are required, generate them from the source columns.
/// 5. Combine columns from step 1 and step 4, and prune columns to be `output_columns`.
///
/// **NOTE: the [`Projection`] is to be applied for the [`TableSchema`] of the data source.**
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrewhereInfo {
    /// columns to be output by prewhere scan
    /// After building [`crate::plan::DataSourcePlan`],
    /// we can get the output schema after projection by `output_columns` from the plan directly.
    pub output_columns: Projection,
    /// columns of prewhere reading stage.
    pub prewhere_columns: Projection,
    /// columns of remain reading stage.
    pub remain_columns: Projection,
    /// filter for prewhere
    /// Assumption: expression's data type must be `DataType::Boolean`.
    pub filter: RemoteExpr<String>,
    /// Optional prewhere virtual columns
    pub virtual_columns: Option<Vec<VirtualColumnInfo>>,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct PushDownInfo {
    /// Optional column indices to use as a projection.
    /// It represents the columns to be read from the source.
    pub projection: Option<Projection>,
    /// Optional column indices as output by the scan, only used when having virtual columns.
    /// The difference with `projection` is the removal of the source columns
    /// which were only used to generate virtual columns.
    pub output_columns: Option<Projection>,
    /// Optional filter expression plan
    /// Assumption: expression's data type must be `DataType::Boolean`.
    pub filter: Option<RemoteExpr<String>>,
    /// Optional prewhere information
    /// used for prewhere optimization
    pub prewhere: Option<PrewhereInfo>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
    /// Optional order_by expression plan, asc, null_first
    pub order_by: Vec<(RemoteExpr<String>, bool, bool)>,
    /// Optional virtual columns
    pub virtual_columns: Option<Vec<VirtualColumnInfo>>,
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
    pub fn top_k(
        &self,
        schema: &TableSchema,
        cluster_key: Option<&String>,
        support: fn(&DataType) -> bool,
    ) -> Option<TopK> {
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
                if !support(&field.data_type().into()) {
                    return None;
                }

                // Only do topk in storage for cluster key.

                if let Some(cluster_key) = cluster_key.as_ref() {
                    if !cluster_key.contains(id) {
                        return None;
                    }
                }

                let leaf_fields = schema.leaf_fields();
                let column_id = leaf_fields
                    .iter()
                    .find(|&p| p == field)
                    .unwrap()
                    .column_id();

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
            let indices = (0..schema.fields().len()).collect::<Vec<usize>>();
            Projection::Columns(indices)
        }
    }
}
