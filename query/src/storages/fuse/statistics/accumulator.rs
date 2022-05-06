//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;

use common_arrow::parquet::FileMetaData;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::eval_aggr;

use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::ColumnMeta;
use crate::storages::fuse::meta::Compression;
use crate::storages::fuse::meta::Versioned;
use crate::storages::index::ClusterStatistics;
use crate::storages::index::ColumnStatistics;
use crate::storages::index::ColumnsStatistics;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_statistics: Vec<ColumnsStatistics>,
    pub cluster_statistics: Vec<Option<ClusterStatistics>>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn begin(
        mut self,
        block: &DataBlock,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<PartiallyAccumulated> {
        let row_count = block.num_rows() as u64;
        let block_in_memory_size = block.memory_size() as u64;

        self.summary_block_count += 1;
        self.summary_row_count += row_count;
        self.in_memory_size += block_in_memory_size;
        let block_stats = Self::acc_columns(block)?;
        self.blocks_statistics.push(block_stats.clone());
        self.cluster_statistics.push(cluster_stats.clone());
        Ok(PartiallyAccumulated {
            accumulator: self,
            block_row_count: block.num_rows() as u64,
            block_size: block.memory_size() as u64,
            block_columns_statistics: block_stats,
            block_cluster_statistics: cluster_stats,
        })
    }

    pub fn add_block(
        &mut self,
        file_size: u64,
        meta: FileMetaData,
        statistics: BlockStatistics,
    ) -> Result<()> {
        self.file_size += file_size;
        self.summary_block_count += 1;
        self.in_memory_size += statistics.block_bytes_size;
        self.summary_row_count += statistics.block_rows_size;
        self.blocks_statistics
            .push(statistics.block_column_statistics.clone());
        self.cluster_statistics
            .push(statistics.block_cluster_statistics.clone());

        self.blocks_metas.push(BlockMeta {
            file_size,
            compression: Compression::Lz4Raw,
            row_count: statistics.block_rows_size,
            block_size: statistics.block_bytes_size,
            col_stats: statistics.block_column_statistics.clone(),
            location: (statistics.block_file_location, DataBlock::VERSION),
            col_metas: Self::column_metas(&meta)?,
            cluster_stats: statistics.block_cluster_statistics,
        });

        Ok(())
    }

    fn column_metas(file_meta: &FileMetaData) -> Result<HashMap<ColumnId, ColumnMeta>> {
        // currently we use one group only
        let num_row_groups = file_meta.row_groups.len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetError(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups[0];
        let mut col_metas = HashMap::with_capacity(row_group.columns.len());
        for (idx, col_chunk) in row_group.columns.iter().enumerate() {
            match &col_chunk.meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;
                    let res = ColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    col_metas.insert(idx as u32, res);
                }
                None => {
                    return Err(ErrorCode::ParquetError(format!(
                        "invalid parquet file, meta data of column idx {} is empty",
                        idx
                    )));
                }
            }
        }
        Ok(col_metas)
    }

    pub fn summary(&self) -> Result<ColumnsStatistics> {
        super::reduce_block_stats(&self.blocks_statistics)
    }

    pub fn summary_clusters(&self) -> Option<ClusterStatistics> {
        super::reduce_cluster_stats(&self.cluster_statistics)
    }

    pub fn acc_columns(data_block: &DataBlock) -> common_exception::Result<ColumnsStatistics> {
        let mut statistics = ColumnsStatistics::new();

        let rows = data_block.num_rows();
        for idx in 0..data_block.num_columns() {
            let col = data_block.column(idx);
            let field = data_block.schema().field(idx);
            let column_field = ColumnWithField::new(col.clone(), field.clone());

            let mut min = DataValue::Null;
            let mut max = DataValue::Null;

            let mins = eval_aggr("min", vec![], &[column_field.clone()], rows)?;
            let maxs = eval_aggr("max", vec![], &[column_field], rows)?;

            if mins.len() > 0 {
                min = mins.get(0);
            }
            if maxs.len() > 0 {
                max = maxs.get(0);
            }
            let (is_all_null, bitmap) = col.validity();
            let null_count = match (is_all_null, bitmap) {
                (true, _) => rows,
                (false, Some(bitmap)) => bitmap.null_count(),
                (false, None) => 0,
            };

            let in_memory_size = col.memory_size() as u64;
            let col_stats = ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size,
            };

            statistics.insert(idx as u32, col_stats);
        }
        Ok(statistics)
    }
}

pub struct PartiallyAccumulated {
    accumulator: StatisticsAccumulator,
    block_row_count: u64,
    block_size: u64,
    block_columns_statistics: HashMap<ColumnId, ColumnStatistics>,
    block_cluster_statistics: Option<ClusterStatistics>,
}

impl PartiallyAccumulated {
    pub fn end(
        mut self,
        file_size: u64,
        location: String,
        col_metas: HashMap<ColumnId, ColumnMeta>,
    ) -> StatisticsAccumulator {
        let mut stats = &mut self.accumulator;
        stats.file_size += file_size;
        let block_meta = BlockMeta {
            row_count: self.block_row_count,
            block_size: self.block_size,
            file_size,
            col_stats: self.block_columns_statistics,
            col_metas,
            cluster_stats: self.block_cluster_statistics,
            location: (location, DataBlock::VERSION),
            compression: Compression::Lz4Raw,
        };
        stats.blocks_metas.push(block_meta);
        self.accumulator
    }
}

pub struct BlockStatistics {
    pub block_rows_size: u64,
    pub block_bytes_size: u64,
    pub block_file_location: String,
    pub block_column_statistics: HashMap<ColumnId, ColumnStatistics>,
    pub block_cluster_statistics: Option<ClusterStatistics>,
}

impl BlockStatistics {
    pub fn from(
        data_block: &DataBlock,
        location: String,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<BlockStatistics> {
        Ok(BlockStatistics {
            block_file_location: location,
            block_rows_size: data_block.num_rows() as u64,
            block_bytes_size: data_block.memory_size() as u64,
            block_column_statistics: Self::columns_statistics(data_block)?,
            block_cluster_statistics: cluster_stats,
        })
    }

    pub fn columns_statistics(data_block: &DataBlock) -> Result<ColumnsStatistics> {
        let mut statistics = ColumnsStatistics::new();

        let rows = data_block.num_rows();
        for idx in 0..data_block.num_columns() {
            let col = data_block.column(idx);
            let field = data_block.schema().field(idx);
            let column_field = ColumnWithField::new(col.clone(), field.clone());

            let mut min = DataValue::Null;
            let mut max = DataValue::Null;

            let mins = eval_aggr("min", vec![], &[column_field.clone()], rows)?;
            let maxs = eval_aggr("max", vec![], &[column_field], rows)?;

            if mins.len() > 0 {
                min = mins.get(0);
            }
            if maxs.len() > 0 {
                max = maxs.get(0);
            }
            let (is_all_null, bitmap) = col.validity();
            let null_count = match (is_all_null, bitmap) {
                (true, _) => rows,
                (false, Some(bitmap)) => bitmap.null_count(),
                (false, None) => 0,
            };

            let in_memory_size = col.memory_size() as u64;
            let col_stats = ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size,
            };

            statistics.insert(idx as u32, col_stats);
        }
        Ok(statistics)
    }

    pub fn clusters_statistics(
        cluster_keys: Vec<usize>,
        block: DataBlock,
    ) -> Result<Option<ClusterStatistics>> {
        if cluster_keys.is_empty() {
            return Ok(None);
        }

        let mut min = Vec::with_capacity(cluster_keys.len());
        let mut max = Vec::with_capacity(cluster_keys.len());

        for key in cluster_keys.iter() {
            let col = block.column(*key);
            min.push(col.get_checked(0)?);
            max.push(col.get_checked(col.len() - 1)?);
        }

        Ok(Some(ClusterStatistics { min, max }))
    }
}
