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

    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,
    pub tbl_args: Option<Vec<Expression>>,
    pub push_downs: Option<Extras>,
    pub benefit_column_prune: bool,
}

impl ReadDataSourcePlan {
    /// Return schema after the projection
    ///
    /// After optimization, only a sub set of the fields in `table_info.schema().fields` are needed.
    /// This is stored inside push_downs
    pub fn schema(&self) -> DataSchemaRef {
        if !self.benefit_column_prune {
            self.table_info.schema()
        } else {
            self.push_downs
                .clone()
                .and_then(|x| x.projection)
                .map(|project_indexes| Arc::new(self.table_info.schema().project(&project_indexes)))
                .unwrap_or_else(|| self.table_info.schema())
        }
    }

    /// Return designated required fields or all fields in a hash map.
    pub fn scan_fields(&self) -> BTreeMap<usize, DataField> {
        if !self.benefit_column_prune {
            self.table_info.schema().fields_map()
        } else {
            self.push_downs
                .clone()
                .and_then(|x| x.projection)
                .map(|project_indexes| {
                    let fields = project_indexes
                        .iter()
                        .map(|i| self.table_info.schema().field(*i).clone());

                    (project_indexes.iter().cloned().zip(fields)).collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_else(|| self.table_info.schema().fields_map())
        }
    }
}
