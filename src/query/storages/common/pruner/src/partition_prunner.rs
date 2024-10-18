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

use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

pub struct PartitionPruner {
    pub filter: Expr<String>,
    pub partition_schema: Arc<TableSchema>,

    leaf_fields: Vec<TableField>,

    pub range_filter: RangeIndex,
}

pub trait FetchPartitionScalars<T> {
    fn eval(_item: &T, _partition_fields: &[TableField]) -> Result<Vec<Scalar>>;
}

impl PartitionPruner {
    pub fn try_create(
        ctx: FunctionContext,
        filter: Expr<String>,
        partition_schema: Arc<TableSchema>,
        full_schema: Arc<TableSchema>,
    ) -> Result<Self> {
        let range_filter = RangeIndex::try_create(
            ctx,
            &filter,
            full_schema.clone(),
            StatisticsOfColumns::default(),
        )?;
        Ok(PartitionPruner {
            filter,
            partition_schema,
            leaf_fields: full_schema.leaf_fields(),
            range_filter,
        })
    }

    pub fn prune<T, F>(&self, partitions: Vec<T>) -> Result<Vec<T>>
    where F: FetchPartitionScalars<T> {
        let filtered_partitions = partitions
            .into_iter()
            .filter(|p| self.should_keep::<T, F>(p).unwrap_or(true))
            .collect();
        Ok(filtered_partitions)
    }

    pub fn should_keep<T, F>(&self, partition: &T) -> Result<bool>
    where F: FetchPartitionScalars<T> {
        let scalars = F::eval(partition, &self.partition_schema.fields)?;
        let mut stats = HashMap::new();

        for (index, scalar) in scalars.into_iter().enumerate() {
            let null_count = u64::from(scalar.is_null());
            let column_stats = ColumnStatistics::new(scalar.clone(), scalar, null_count, 0, None);

            let mut f = self
                .leaf_fields
                .iter()
                .filter(|f| f.name() == &self.partition_schema.field(index).name);

            if let Some(f) = f.next() {
                stats.insert(f.column_id(), column_stats);
            }
        }
        self.range_filter.apply(&stats, |_| false)
    }
}
