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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::FuseTable;

pub struct FuseStatistic<'a> {
    pub table: &'a FuseTable,
}

impl<'a> FuseStatistic<'a> {
    pub fn new(table: &'a FuseTable) -> Self {
        Self { table }
    }

    #[async_backtrace::framed]
    pub async fn get_statistic(self) -> Result<DataBlock> {
        let snapshot_opt = self.table.read_table_snapshot().await?;
        if let Some(snapshot) = snapshot_opt {
            let table_statistics = self
                .table
                .read_table_snapshot_statistics(Some(&snapshot))
                .await?;
            return self.to_block(&snapshot.summary, &table_statistics);
        }
        Ok(DataBlock::empty_with_schema(Arc::new(
            FuseStatistic::schema().into(),
        )))
    }

    fn to_block(
        &self,
        _summy: &Statistics,
        table_statistics: &Option<Arc<TableSnapshotStatistics>>,
    ) -> Result<DataBlock> {
        let mut col_names = vec![];
        let mut col_ndvs = vec![];
        let mut col_his = vec![];
        if let Some(table_statistics) = table_statistics {
            for (i, n) in table_statistics.column_distinct_values().iter() {
                // Get column name by column id
                let table_filed = self.table.table_info.meta.schema.field_of_column_id(*i)?;
                col_names.push(table_filed.name.clone());
                col_ndvs.push(*n);
                let his_info = table_statistics.histograms.get(i);
                if let Some(his_info) = his_info {
                    let mut his_infos = vec![];
                    for (i, bucket) in his_info.buckets.iter().enumerate() {
                        let min = bucket.lower_bound().to_string()?;
                        let max = bucket.upper_bound().to_string()?;
                        let ndv = bucket.num_distinct();
                        let count = bucket.num_values();
                        let his_info = format!(
                            "[bucket id: {:?}, min: {:?}, max: {:?}, ndv: {:?}, count: {:?}]",
                            i, min, max, ndv, count
                        );
                        his_infos.push(his_info);
                    }
                    col_his.push(his_infos.join(", "));
                } else {
                    col_his.push("".to_string());
                }
            }
        };

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(col_names),
            UInt64Type::from_data(col_ndvs),
            StringType::from_data(col_his),
        ]))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("column_name", TableDataType::String),
            TableField::new(
                "distinct_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("histogram", TableDataType::String),
        ])
    }
}
