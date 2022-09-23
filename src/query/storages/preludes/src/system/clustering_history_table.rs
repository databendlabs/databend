// Copyright 2022 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::system::SystemLogElement;
use crate::system::SystemLogQueue;
use crate::system::SystemLogTable;

#[derive(Clone)]
pub struct ClusteringHistoryLogElement {
    pub start_time: i64,
    pub end_time: i64,
    pub database: String,
    pub table: String,
    pub reclustered_bytes: u64,
    pub reclustered_rows: u64,
}

impl SystemLogElement for ClusteringHistoryLogElement {
    const TABLE_NAME: &'static str = "clustering_history";

    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("start_time", TimestampType::new_impl(3)),
            DataField::new("end_time", TimestampType::new_impl(3)),
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("table", Vu8::to_data_type()),
            DataField::new("reclustered_bytes", u64::to_data_type()),
            DataField::new("reclustered_rows", u64::to_data_type()),
        ])
    }

    fn fill_to_data_block(&self, columns: &mut Vec<Box<dyn MutableColumn>>) -> Result<()> {
        let mut columns = columns.iter_mut();
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.start_time))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.end_time))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.database.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.table.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.reclustered_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.reclustered_rows))
    }
}

pub type ClusteringHistoryQueue = SystemLogQueue<ClusteringHistoryLogElement>;
pub type ClusteringHistoryTable = SystemLogTable<ClusteringHistoryLogElement>;
