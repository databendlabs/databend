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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;

use crate::io::read::load_spatial_index_files;

pub struct SpatialIndexReadResult {
    pub columns: Vec<Column>,
    pub column_id_to_index: HashMap<ColumnId, usize>,
}

#[derive(Clone)]
pub struct SpatialIndexReader {
    operator: Operator,
    settings: ReadSettings,
    column_id_to_index: HashMap<ColumnId, usize>,
    column_names: Vec<String>,
}

impl SpatialIndexReader {
    pub fn create(operator: Operator, settings: ReadSettings, column_ids: Vec<ColumnId>) -> Self {
        let mut column_id_to_index = HashMap::with_capacity(column_ids.len());
        let mut column_names = Vec::with_capacity(column_ids.len() * 2);
        for column_id in &column_ids {
            column_id_to_index.insert(*column_id, column_names.len());
            column_names.push(format!("{column_id}"));
        }

        Self {
            operator,
            settings,
            column_id_to_index,
            column_names,
        }
    }

    pub async fn read(&self, location: &str) -> Result<SpatialIndexReadResult> {
        let columns = load_spatial_index_files(
            self.operator.clone(),
            &self.settings,
            &self.column_names,
            location,
        )
        .await?;

        let result = SpatialIndexReadResult {
            columns,
            column_id_to_index: self.column_id_to_index.clone(),
        };
        Ok(result)
    }
}
