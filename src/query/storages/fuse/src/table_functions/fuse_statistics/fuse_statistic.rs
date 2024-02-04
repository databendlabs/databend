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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::Statistics;

use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseStatistic<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
}

impl<'a> FuseStatistic<'a> {
    pub fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    #[async_backtrace::framed]
    pub async fn get_statistic(self) -> Result<DataBlock> {
        let snapshot_opt = self.table.read_table_snapshot().await?;
        if let Some(snapshot) = snapshot_opt {
            return self.to_block(&snapshot.summary);
        }
        Ok(DataBlock::empty_with_schema(Arc::new(
            FuseStatistic::schema().into(),
        )))
    }

    fn to_block(&self, summary: &Statistics) -> Result<DataBlock> {
        let mut col_ids = vec![];
        let mut col_ndvs = vec![];

        for f in self.table.schema().fields.iter() {
            col_ids.push(f.column_id);

            if let Some(n) = summary.col_stats.get(&f.column_id) {
                col_ndvs.push(n.unify_distinct_value().unwrap_or(summary.row_count));
            } else {
                col_ndvs.push(summary.row_count);
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            UInt32Type::from_data(col_ids),
            UInt64Type::from_data(col_ndvs),
        ]))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("column_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("ndv", TableDataType::Number(NumberDataType::UInt64)),
        ])
    }
}
