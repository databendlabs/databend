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
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_meta_app::schema::TableInfo;
use common_meta_types::UserStageInfo;

use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::Projection;
use crate::plan::PushDownInfo;
use crate::plan::StageFileInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct StageTableInfo {
    pub schema: DataSchemaRef,
    pub path: String,
    pub files: Vec<String>,
    pub pattern: String,
    pub user_stage_info: UserStageInfo,
    pub files_to_copy: Option<Vec<StageFileInfo>>,
}

impl StageTableInfo {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn desc(&self) -> String {
        self.user_stage_info.stage_name.clone()
    }
}

impl Debug for StageTableInfo {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.user_stage_info)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DataSourceInfo {
    // Normal table source, `fuse/system`.
    TableSource(TableInfo),

    // Internal/External source, like `s3://` or `azblob://`.
    StageSource(StageTableInfo),
}

impl DataSourceInfo {
    pub fn schema(&self) -> Arc<DataSchema> {
        match self {
            DataSourceInfo::TableSource(table_info) => table_info.schema(),
            DataSourceInfo::StageSource(table_info) => table_info.schema(),
        }
    }

    pub fn desc(&self) -> String {
        match self {
            DataSourceInfo::TableSource(table_info) => table_info.desc.clone(),
            DataSourceInfo::StageSource(table_info) => table_info.desc(),
        }
    }
}

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
    pub scan_fields: Option<BTreeMap<usize, DataField>>,

    pub parts: Partitions,
    pub statistics: PartStatistics,
    pub description: String,

    pub tbl_args: Option<Vec<DataValue>>,
    pub push_downs: Option<PushDownInfo>,
}

impl DataSourcePlan {
    /// Return schema after the projection
    pub fn schema(&self) -> DataSchemaRef {
        self.scan_fields
            .clone()
            .map(|x| {
                let fields: Vec<_> = x.values().cloned().collect();
                Arc::new(self.source_info.schema().project_by_fields(fields))
            })
            .unwrap_or_else(|| self.source_info.schema())
    }

    /// Return designated required fields or all fields in a hash map.
    pub fn scan_fields(&self) -> BTreeMap<usize, DataField> {
        self.scan_fields
            .clone()
            .unwrap_or_else(|| self.source_info.schema().fields_map())
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
