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

use common_datavalues::prelude::*;
use common_meta_app::schema::TableInfo;
use once_cell::sync::Lazy;

use crate::plan::Expression;
use crate::plan::Projection;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StageKind {
    Normal,
    Expansive,
    Merge,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrewhereInfo {
    /// columns to be ouput be prewhere scan
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
pub struct Extras {
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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,
    /// Number of partitions scanned, (after pruning)
    pub partitions_scanned: usize,
    /// Number of partitions, (before pruning)
    pub partitions_total: usize,
    /// Is the statistics exact.
    pub is_exact: bool,
}

impl Statistics {
    pub fn new_estimated(
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: false,
        }
    }

    pub fn new_exact(
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: true,
        }
    }

    pub fn default_exact() -> Self {
        Self {
            is_exact: true,
            ..Default::default()
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn get_description(&self, table_info: &TableInfo) -> String {
        if self.read_rows > 0 {
            format!(
                "(Read from {} table, {} Read Rows:{}, Read Bytes:{}, Partitions Scanned:{}, Partitions Total:{})",
                table_info.desc,
                if self.is_exact {
                    "Exactly"
                } else {
                    "Approximately"
                },
                self.read_rows,
                self.read_bytes,
                self.partitions_scanned,
                self.partitions_total,
            )
        } else {
            format!("(Read from {} table)", table_info.desc)
        }
    }
}

pub static SINK_SCHEMA: Lazy<DataSchemaRef> = Lazy::new(|| {
    DataSchemaRefExt::create(vec![
        DataField::new("seg_loc", Vu8::to_data_type()),
        DataField::new("seg_info", Vu8::to_data_type()),
    ])
});
