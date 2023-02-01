// Copyright 2023 Datafuse Labs.
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
use std::sync::Arc;

use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchema;

use crate::plan::datasource::datasource_info::DataSourceInfo;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::Projection;
use crate::plan::PushDownInfo;

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DataSourcePlan {
    // TODO catalog id is better
    pub catalog: String,
    pub source_info: DataSourceInfo,

    /// Required fields to scan.
    ///
    /// After optimization, only a sub set of the fields in `table_info.schema().fields` are needed.
    /// The key is the column_index of `ColumnEntry` in `Metadata`.
    ///
    /// If it is None, one should use `table_info.schema().fields()`.
    pub scan_fields: Option<BTreeMap<usize, TableField>>,

    pub parts: Partitions,
    pub statistics: PartStatistics,
    pub description: String,

    pub tbl_args: Option<Vec<Scalar>>,
    pub push_downs: Option<PushDownInfo>,
}

impl DataSourcePlan {
    /// Return schema after the projection
    pub fn schema(&self) -> Arc<TableSchema> {
        self.scan_fields
            .clone()
            .map(|x| Arc::new(self.source_info.schema().project_by_fields(&x)))
            .unwrap_or_else(|| self.source_info.schema())
    }

    pub fn projections(&self) -> Projection {
        let default_proj = || {
            (0..self.source_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        if let Some(PushDownInfo {
            projection: Some(prj),
            ..
        }) = &self.push_downs
        {
            prj.clone()
        } else {
            Projection::Columns(default_proj())
        }
    }
}
