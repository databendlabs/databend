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

use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;

use crate::SystemLogElement;
use crate::SystemLogQueue;
use crate::SystemLogTable;

#[derive(Clone)]
pub struct ClusteringHistoryLogElement {
    pub start_time: i64,
    pub end_time: i64,
    pub database: String,
    pub table: String,
    pub byte_size: u64,
    pub row_count: u64,
}

impl SystemLogElement for ClusteringHistoryLogElement {
    const TABLE_NAME: &'static str = "clustering_history";

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("start_time", TableDataType::Timestamp),
            TableField::new("end_time", TableDataType::Timestamp),
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("byte_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
        ])
    }

    fn fill_to_data_block(&self, columns: &mut Vec<ColumnBuilder>) -> Result<()> {
        let mut columns = columns.iter_mut();
        columns
            .next()
            .unwrap()
            .push(Scalar::Timestamp(self.start_time).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Timestamp(self.end_time).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.database.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.table.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.byte_size)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.row_count)).as_ref());
        Ok(())
    }
}

pub type ClusteringHistoryQueue = SystemLogQueue<ClusteringHistoryLogElement>;
pub type ClusteringHistoryTable = SystemLogTable<ClusteringHistoryLogElement>;
