// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_meta_types::TableInfo;

use crate::Expression;
use crate::Extras;
use crate::Partitions;
use crate::Statistics;

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    pub table_info: TableInfo,

    /// Required fields to scan.
    ///
    /// After optimization, only a sub set of the fields in `table_info.schema().fields` are needed.
    /// The key is the index of the field in original `table_info.schema().fields`.
    ///
    /// If it is None, one should use `table_info.schema().fields()`.
    pub scan_fields: Option<BTreeMap<usize, DataField>>,

    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,

    pub tbl_args: Option<Vec<Expression>>,
    pub push_downs: Option<Extras>,
}

impl ReadDataSourcePlan {
    /// Return schema after the projection
    pub fn schema(&self) -> DataSchemaRef {
        self.scan_fields
            .clone()
            .map(|x| {
                let fields: Vec<_> = x.iter().map(|(_, f)| f.clone()).collect();
                Arc::new(self.table_info.schema().project(fields))
            })
            .unwrap_or_else(|| self.table_info.schema())
    }

    /// Return designated required fields or all fields in a hash map.
    pub fn scan_fields(&self) -> BTreeMap<usize, DataField> {
        self.scan_fields
            .clone()
            .unwrap_or_else(|| self.table_info.schema().fields_map())
    }
}
