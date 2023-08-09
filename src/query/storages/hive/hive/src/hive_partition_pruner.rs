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
use std::sync::Arc;
use std::vec;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Expr;
use common_expression::TableSchema;
use log::debug;
use storages_common_index::RangeIndex;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::utils::str_field_to_scalar;

pub struct HivePartitionPruner {
    pub ctx: Arc<dyn TableContext>,
    pub filter: Expr<String>,
    // pub partitions: Vec<String>,
    pub partition_schema: Arc<TableSchema>,
    pub full_schema: Arc<TableSchema>,
}

impl HivePartitionPruner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        filter: Expr<String>,
        partition_schema: Arc<TableSchema>,
        full_schema: Arc<TableSchema>,
    ) -> Self {
        HivePartitionPruner {
            ctx,
            filter,
            partition_schema,
            full_schema,
        }
    }

    pub fn get_column_stats(&self, partitions: &Vec<String>) -> Result<Vec<StatisticsOfColumns>> {
        let mut data = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let mut stats = HashMap::new();
            for (index, singe_value) in partition.split('/').enumerate() {
                let kv = singe_value.split('=').collect::<Vec<&str>>();
                let field = self.partition_schema.fields()[index].clone();
                let scalar = str_field_to_scalar(kv[1], &field.data_type().into())?;
                let null_count = u64::from(scalar.is_null());
                let column_stats =
                    ColumnStatistics::new(scalar.clone(), scalar, null_count, 0, None);
                stats.insert(index as u32, column_stats);
            }
            data.push(stats);
        }

        Ok(data)
    }

    pub fn prune(&self, partitions: Vec<String>) -> Result<Vec<String>> {
        let range_filter = RangeIndex::try_create(
            self.ctx.get_function_context()?,
            &self.filter,
            self.full_schema.clone(),
            StatisticsOfColumns::default(),
        )?;
        let column_stats = self.get_column_stats(&partitions)?;
        let mut filtered_partitions = vec![];
        for (idx, stats) in column_stats.into_iter().enumerate() {
            let block_stats = stats
                .iter()
                .map(|(k, v)| {
                    let partition_col_name = self.partition_schema.field(*k as usize).name();
                    let index = self.full_schema.index_of(partition_col_name).unwrap();

                    (index as u32, v.clone())
                })
                .collect();

            if range_filter.apply(&block_stats, |_| false)? {
                filtered_partitions.push(partitions[idx].clone());
            }
        }
        debug!("hive pruned partitions: {:?}", filtered_partitions);
        Ok(filtered_partitions)
    }
}
