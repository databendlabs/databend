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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::HILBERT_CLUSTER_DIMENSIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::table::ClusterType;

use crate::FuseTable;
use crate::statistics::aggregate_cluster_key_min_max;

/// Builds cluster statistics for the streaming block writer.
///
/// Streaming currently supports only Hilbert clustering. Linear and vector layouts still use the
/// regular append pipeline, where rows are sorted before block serialization.
#[derive(Clone, Default)]
pub struct HilbertStatisticsBuilder {
    cluster_key_id: Option<u32>,
    dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
    temporary_column_count: usize,
    level: i32,
    operators: Vec<BlockOperator>,
    func_ctx: FunctionContext,
}

impl HilbertStatisticsBuilder {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        source_schema: &TableSchemaRef,
    ) -> Result<Arc<Self>> {
        let Some(cluster_type) = table.cluster_type() else {
            return Ok(Arc::new(Self::default()));
        };
        if cluster_type != ClusterType::Hilbert {
            return Err(ErrorCode::Internal(
                "stream block writing only supports Hilbert cluster statistics",
            ));
        }

        let input_schema: Arc<DataSchema> = DataSchema::from(source_schema).into();
        let generator =
            table.get_cluster_stats_gen(ctx, 0, table.get_block_thresholds(), input_schema)?;
        if !generator.is_hilbert() {
            return Err(ErrorCode::Internal(
                "Hilbert stream writer requires Hilbert cluster dimensions",
            ));
        }
        let dimension_offsets = generator.hilbert_dimension_offsets()?;

        Ok(Arc::new(Self {
            cluster_key_id: table.cluster_key_id(),
            dimension_offsets,
            temporary_column_count: generator.extra_key_num,
            level: 0,
            operators: generator.eval_operators,
            func_ctx: generator.func_ctx,
        }))
    }

    /// Create a builder for recluster input whose dimension expressions and trailing Hilbert sort
    /// key are already evaluated.
    pub fn for_recluster(
        cluster_key_id: u32,
        dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
        temporary_column_count: usize,
        level: i32,
    ) -> Arc<Self> {
        Arc::new(Self {
            cluster_key_id: Some(cluster_key_id),
            dimension_offsets,
            temporary_column_count,
            level,
            operators: Vec::new(),
            func_ctx: FunctionContext::default(),
        })
    }
}

pub struct HilbertStatisticsState {
    builder: Arc<HilbertStatisticsBuilder>,
    mins: Vec<Scalar>,
    maxs: Vec<Scalar>,
}

impl HilbertStatisticsState {
    pub fn new(builder: Arc<HilbertStatisticsBuilder>) -> Self {
        Self {
            builder,
            mins: Vec::new(),
            maxs: Vec::new(),
        }
    }

    /// Accumulate the Hilbert dimension MBR and remove expression and sort-key columns before the
    /// source block is passed to column statistics, indexes and parquet serialization.
    pub fn add_block(&mut self, input: DataBlock) -> Result<DataBlock> {
        if self.builder.cluster_key_id.is_none() {
            return Ok(input);
        }

        let mut block = self
            .builder
            .operators
            .iter()
            .try_fold(input, |block, operator| {
                operator.execute(&self.builder.func_ctx, block)
            })?;

        for (dimension, offset) in self.builder.dimension_offsets.iter().copied().enumerate() {
            let (min, max, _) = aggregate_cluster_key_min_max(&block, offset)?;
            if dimension == self.mins.len() {
                self.mins.push(min);
                self.maxs.push(max);
            } else {
                if min.as_ref().cmp(&self.mins[dimension].as_ref()) == Ordering::Less {
                    self.mins[dimension] = min;
                }
                if max.as_ref().cmp(&self.maxs[dimension].as_ref()) == Ordering::Greater {
                    self.maxs[dimension] = max;
                }
            }
        }

        if self.builder.temporary_column_count > block.num_columns() {
            return Err(ErrorCode::Internal(format!(
                "Hilbert stream writer expected at least {} temporary columns, got {} columns",
                self.builder.temporary_column_count,
                block.num_columns()
            )));
        }
        block.pop_columns(self.builder.temporary_column_count);
        Ok(block)
    }

    pub fn finalize(self, large_enough: bool) -> Result<Option<ClusterStatistics>> {
        let Some(cluster_key_id) = self.builder.cluster_key_id else {
            return Ok(None);
        };
        if self.mins.len() != HILBERT_CLUSTER_DIMENSIONS
            || self.maxs.len() != HILBERT_CLUSTER_DIMENSIONS
        {
            return Err(ErrorCode::Internal(
                "Hilbert stream writer finalized without complete dimension statistics",
            ));
        }

        // A block whose Hilbert dimensions are all constant (min == max on every dimension) is
        // already clustered to a single point and cannot be improved by further recluster. Mark it
        // with level -1 so it is permanently excluded from recluster task selection, mirroring the
        // linear path in `ClusterStatsGenerator`. Only large-enough blocks are frozen; small
        // constant blocks must still be eligible for compaction (checked by the caller).
        let level = if large_enough && self.mins == self.maxs {
            -1
        } else {
            self.builder.level
        };

        Ok(Some(ClusterStatistics::new(
            cluster_key_id,
            self.mins,
            self.maxs,
            level,
        )))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::UInt32Type;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn int32_scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    #[test]
    fn test_hilbert_state_accumulates_dimension_mbr_across_blocks() -> Result<()> {
        let builder = Arc::new(HilbertStatisticsBuilder {
            cluster_key_id: Some(7),
            dimension_offsets: [0, 1],
            temporary_column_count: 1,
            level: 3,
            operators: vec![],
            func_ctx: FunctionContext::default(),
        });
        let mut state = HilbertStatisticsState::new(builder);

        let first = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![3, 1]),
            Int32Type::from_data(vec![8, 4]),
            UInt32Type::from_data(vec![0, 1]),
        ]);
        let second = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![2, 6]),
            Int32Type::from_data(vec![9, 5]),
            UInt32Type::from_data(vec![2, 3]),
        ]);

        assert_eq!(state.add_block(first)?.num_columns(), 2);
        assert_eq!(state.add_block(second)?.num_columns(), 2);
        let stats = state.finalize(true)?.unwrap();

        assert_eq!(stats.cluster_key_id, 7);
        assert_eq!(stats.min, vec![int32_scalar(1), int32_scalar(4)]);
        assert_eq!(stats.max, vec![int32_scalar(6), int32_scalar(9)]);
        assert_eq!(stats.level, 3);
        Ok(())
    }

    #[test]
    fn test_unclustered_state_is_noop() -> Result<()> {
        let builder = Arc::new(HilbertStatisticsBuilder::default());
        let mut state = HilbertStatisticsState::new(builder);
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2])]);

        assert_eq!(state.add_block(block)?.num_rows(), 2);
        assert!(state.finalize(false)?.is_none());
        Ok(())
    }
}
