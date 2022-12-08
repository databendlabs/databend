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

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::RemoteExpr;
use common_expression::types::DataType;
use common_expression::DataSchema;
use common_expression::Scalar;
use common_planner::Expression;
use common_storages_index::range_filter::RangeFilter;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::StatisticsOfColumns;

use crate::hive_table::HIVE_DEFAULT_PARTITION;
use crate::utils::str_field_to_scalar;

pub struct HivePartitionPruner {
    pub ctx: Arc<dyn TableContext>,
    pub filters: Vec<RemoteExpr<String>>,
    // pub partitions: Vec<String>,
    pub partition_schema: Arc<DataSchema>,
}

impl HivePartitionPruner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        filters: Vec<RemoteExpr<String>>,
        partition_schema: Arc<DataSchema>,
    ) -> Self {
        HivePartitionPruner {
            ctx,
            filters,
            partition_schema,
        }
    }

    pub fn get_column_stats(&self, partitions: &Vec<String>) -> Result<Vec<StatisticsOfColumns>> {
        let mut datas = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let mut stats = HashMap::new();
            for (index, singe_value) in partition.split('/').enumerate() {
                let kv = singe_value.split('=').collect::<Vec<&str>>();
                let field = self.partition_schema.fields()[index].clone();
                let scalar = str_field_to_scalar(&kv[1], &field.data_type().into())?;
                let column_stats = ColumnStatistics {
                    min: scalar.clone(),
                    max: scalar,
                    null_count: 0,
                    in_memory_size: 0,
                    distinct_of_values: None,
                };
                stats.insert(index as u32, column_stats);
            }
            datas.push(stats);
        }

        Ok(datas)
    }

    pub fn prune(&self, partitions: Vec<String>) -> Result<Vec<String>> {
        let range_filter = RangeFilter::try_create(
            self.ctx.clone(),
            &self.filters,
            self.partition_schema.clone(),
        )?;
        let column_stats = self.get_column_stats(&partitions)?;
        let mut filted_partitions = vec![];
        for (idx, stats) in column_stats.into_iter().enumerate() {
            todo!("expression");
        }
        tracing::debug!("hive pruned partitinos: {:?}", filted_partitions);
        Ok(filted_partitions)
    }
}
