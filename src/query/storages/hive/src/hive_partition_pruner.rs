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
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::ColumnStatistics;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_legacy_expression::LegacyExpression;
use common_storages_index::range_filter::RangeFilter;

pub struct HivePartitionPruner {
    pub ctx: Arc<dyn TableContext>,
    pub filters: Vec<LegacyExpression>,
    // pub partitions: Vec<String>,
    pub partition_schema: Arc<DataSchema>,
}

impl HivePartitionPruner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        filters: Vec<LegacyExpression>,
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

                let v = match field.data_type() {
                    DataTypeImpl::String(_) => DataValue::String(kv[1].as_bytes().to_vec()),
                    DataTypeImpl::Int8(_)
                    | DataTypeImpl::Int16(_)
                    | DataTypeImpl::Int32(_)
                    | DataTypeImpl::Int64(_) => DataValue::Int64(kv[1].parse::<i64>().unwrap()),
                    DataTypeImpl::UInt8(_)
                    | DataTypeImpl::UInt16(_)
                    | DataTypeImpl::UInt32(_)
                    | DataTypeImpl::UInt64(_) => DataValue::UInt64(kv[1].parse::<u64>().unwrap()),
                    DataTypeImpl::Float32(_) | DataTypeImpl::Float64(_) => {
                        DataValue::Float64(kv[1].parse::<f64>().unwrap())
                    }
                    _ => {
                        return Err(ErrorCode::UnImplement(format!(
                            "unsupported partition type, {:?}",
                            field
                        )));
                    }
                };

                let column_stats = ColumnStatistics {
                    min: v.clone(),
                    max: v,
                    null_count: 0,
                    in_memory_size: 0,
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
            if range_filter.eval(&stats, 1)? {
                filted_partitions.push(partitions[idx].clone());
            }
        }
        tracing::debug!("hive pruned partitinos: {:?}", filted_partitions);
        Ok(filted_partitions)
    }
}
