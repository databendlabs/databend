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

use std::collections::BTreeSet;
use std::collections::HashMap;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::split_row_id;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_metrics::storage::metrics_inc_row_fetch_affinity_reassigned_blocks;
use databend_common_metrics::storage::metrics_inc_row_fetch_distributed_batches;
use databend_common_metrics::storage::metrics_inc_row_fetch_local_batches;
use log::info;

use super::FlightScatter;

struct RoutingDecision {
    local: bool,
    indices: Vec<u64>,
    distinct_blocks: usize,
    destination_rows: Vec<usize>,
    destination_blocks: Vec<usize>,
    affinity_reassigned_blocks: usize,
}

pub struct AdaptiveRowFetchFlightScatter {
    hash_scatter: Box<dyn FlightScatter>,
    query_id: String,
    row_id_col_offset: usize,
    local_block_threshold: usize,
    local_pos: usize,
    scatter_size: usize,
}

impl AdaptiveRowFetchFlightScatter {
    pub fn create(
        hash_scatter: Box<dyn FlightScatter>,
        query_id: String,
        row_id_col_offset: usize,
        local_block_threshold: usize,
        local_pos: usize,
        scatter_size: usize,
    ) -> Box<dyn FlightScatter> {
        Box::new(Self {
            hash_scatter,
            query_id,
            row_id_col_offset,
            local_block_threshold,
            local_pos,
            scatter_size,
        })
    }

    fn row_id_prefixes(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let entry = data_block
            .columns()
            .get(self.row_id_col_offset)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Adaptive RowFetch row ID column offset {} is out of bounds for {} columns",
                    self.row_id_col_offset,
                    data_block.num_columns()
                ))
            })?;
        let column = entry.to_column();
        let row_ids = match entry.data_type() {
            DataType::Number(NumberDataType::UInt64) => {
                column.into_number().unwrap().into_u_int64().unwrap()
            }
            DataType::Nullable(inner)
                if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
            {
                column
                    .into_nullable()
                    .unwrap()
                    .column
                    .into_number()
                    .unwrap()
                    .into_u_int64()
                    .unwrap()
            }
            data_type => {
                return Err(ErrorCode::Internal(format!(
                    "Adaptive RowFetch row ID column must be UInt64, but got {data_type}"
                )));
            }
        };

        Ok(row_ids
            .iter()
            .map(|row_id| split_row_id(*row_id).0)
            .collect())
    }

    fn local_decision(&self, prefixes: Vec<u64>, distinct_blocks: usize) -> RoutingDecision {
        let mut destination_rows = vec![0; self.scatter_size];
        let mut destination_blocks = vec![0; self.scatter_size];
        destination_rows[self.local_pos] = prefixes.len();
        destination_blocks[self.local_pos] = distinct_blocks;

        RoutingDecision {
            local: true,
            indices: vec![self.local_pos as u64; prefixes.len()],
            distinct_blocks,
            destination_rows,
            destination_blocks,
            affinity_reassigned_blocks: 0,
        }
    }

    fn distributed_decision(
        &self,
        data_block: &DataBlock,
        prefixes: Vec<u64>,
    ) -> Result<RoutingDecision> {
        let primary_indices = self
            .hash_scatter
            .scatter_indices(data_block)?
            .ok_or_else(|| ErrorCode::Internal("RowFetch hash scatter does not expose indices"))?;
        if primary_indices.len() != prefixes.len() {
            return Err(ErrorCode::Internal(format!(
                "RowFetch hash scatter produced {} indices for {} rows",
                primary_indices.len(),
                prefixes.len()
            )));
        }

        let mut blocks = HashMap::<u64, usize>::new();
        let mut destination_rows = vec![0usize; self.scatter_size];
        let mut destination_blocks = vec![0usize; self.scatter_size];
        for (prefix, primary) in prefixes
            .iter()
            .copied()
            .zip(primary_indices.iter().copied())
        {
            let primary = primary as usize;
            if primary >= self.scatter_size {
                return Err(ErrorCode::Internal(format!(
                    "RowFetch hash scatter destination {} is out of bounds for {} destinations",
                    primary, self.scatter_size
                )));
            }

            destination_rows[primary] += 1;
            match blocks.get(&prefix) {
                Some(block_primary) => {
                    if *block_primary != primary {
                        return Err(ErrorCode::Internal(format!(
                            "Rows from RowFetch block {} mapped to different primary destinations",
                            prefix
                        )));
                    }
                }
                None => {
                    blocks.insert(prefix, primary);
                    destination_blocks[primary] += 1;
                }
            }
        }

        let distinct_blocks = blocks.len();

        Ok(RoutingDecision {
            local: false,
            indices: primary_indices,
            distinct_blocks,
            destination_rows,
            destination_blocks,
            affinity_reassigned_blocks: 0,
        })
    }

    fn route(&self, data_block: &DataBlock) -> Result<RoutingDecision> {
        if self.scatter_size == 0 || self.local_pos >= self.scatter_size {
            return Err(ErrorCode::Internal(format!(
                "Invalid adaptive RowFetch destinations: local_pos={}, scatter_size={}",
                self.local_pos, self.scatter_size
            )));
        }

        let prefixes = self.row_id_prefixes(data_block)?;
        let distinct_blocks = prefixes.iter().copied().collect::<BTreeSet<_>>().len();
        if distinct_blocks <= self.local_block_threshold {
            return Ok(self.local_decision(prefixes, distinct_blocks));
        }
        self.distributed_decision(data_block, prefixes)
    }

    fn record_decision(&self, decision: &RoutingDecision, rows: usize) {
        let mode = if decision.local {
            Profile::record_usize_profile(ProfileStatisticsName::RowFetchLocalBatches, 1);
            metrics_inc_row_fetch_local_batches(1);
            "local"
        } else {
            Profile::record_usize_profile(ProfileStatisticsName::RowFetchDistributedBatches, 1);
            Profile::record_usize_profile(
                ProfileStatisticsName::RowFetchAffinityReassignedBlocks,
                decision.affinity_reassigned_blocks,
            );
            metrics_inc_row_fetch_distributed_batches(1);
            metrics_inc_row_fetch_affinity_reassigned_blocks(
                decision.affinity_reassigned_blocks as u64,
            );
            "distributed"
        };

        info!(
            "Adaptive RowFetch routing query_id={} mode={} rows={} distinct_blocks={} local_block_threshold={} destinations={} destination_rows={:?} destination_blocks={:?} affinity_reassigned_blocks={}",
            self.query_id,
            mode,
            rows,
            decision.distinct_blocks,
            self.local_block_threshold,
            self.scatter_size,
            decision.destination_rows,
            decision.destination_blocks,
            decision.affinity_reassigned_blocks
        );
    }
}

impl FlightScatter for AdaptiveRowFetchFlightScatter {
    fn name(&self) -> &'static str {
        "AdaptiveRowFetch"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let decision = self.route(&data_block)?;
        self.record_decision(&decision, data_block.num_rows());

        let block_meta = data_block.get_meta().cloned();
        let blocks = DataBlock::scatter(&data_block, &decision.indices, self.scatter_size)?;
        blocks
            .into_iter()
            .map(|block| block.add_meta(block_meta.clone()))
            .collect()
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        Ok(Some(self.route(data_block)?.indices))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::compute_row_id;
    use databend_common_catalog::plan::compute_row_id_prefix;
    use databend_common_expression::FromData;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    struct PrimaryZeroScatter;

    impl FlightScatter for PrimaryZeroScatter {
        fn name(&self) -> &'static str {
            "PrimaryZero"
        }

        fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
            let indices = self.scatter_indices(&data_block)?.unwrap();
            DataBlock::scatter(&data_block, &indices, 3)
        }

        fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
            Ok(Some(vec![0; data_block.num_rows()]))
        }
    }

    fn row_id_block(block_ids: &[u64]) -> DataBlock {
        let row_ids = block_ids
            .iter()
            .map(|block_id| compute_row_id(compute_row_id_prefix(0, *block_id), 0))
            .collect();
        DataBlock::new_from_columns(vec![UInt64Type::from_data(row_ids)])
    }

    fn scatter(local_block_threshold: usize) -> AdaptiveRowFetchFlightScatter {
        AdaptiveRowFetchFlightScatter {
            hash_scatter: Box::new(PrimaryZeroScatter),
            query_id: "test-query".to_string(),
            row_id_col_offset: 0,
            local_block_threshold,
            local_pos: 1,
            scatter_size: 3,
        }
    }

    #[test]
    fn keeps_compact_row_fetch_local() -> Result<()> {
        let block = row_id_block(&[1, 1, 2, 2]);
        assert_eq!(scatter(2).scatter_indices(&block)?, Some(vec![1, 1, 1, 1]));
        Ok(())
    }

    #[test]
    fn preserves_primary_hash_affinity_for_dispersed_blocks() -> Result<()> {
        let block = row_id_block(&[1, 1, 2, 3, 4]);
        let decision = scatter(2).route(&block)?;

        assert!(!decision.local);
        assert_eq!(decision.distinct_blocks, 4);
        assert_eq!(decision.indices, vec![0; 5]);
        assert_eq!(decision.destination_rows, vec![5, 0, 0]);
        assert_eq!(decision.destination_blocks, vec![4, 0, 0]);
        assert_eq!(decision.affinity_reassigned_blocks, 0);
        Ok(())
    }

    #[test]
    fn does_not_reassign_skewed_primary_blocks() -> Result<()> {
        let mut block_ids = vec![1; 100];
        block_ids.extend(vec![2; 50]);
        block_ids.extend([3, 4]);
        let block = row_id_block(&block_ids);
        let decision = scatter(2).route(&block)?;

        assert_eq!(decision.destination_rows.iter().sum::<usize>(), 152);
        assert_eq!(decision.destination_blocks.iter().sum::<usize>(), 4);
        assert_eq!(decision.destination_rows, vec![152, 0, 0]);
        assert_eq!(decision.destination_blocks, vec![4, 0, 0]);
        assert_eq!(decision.affinity_reassigned_blocks, 0);
        assert_eq!(decision.indices, vec![0; 152]);
        Ok(())
    }
}
