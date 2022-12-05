//  Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;

use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_storages_fuse::ColumnLeaves;
use common_storages_fuse::ColumnMeta;
use common_storages_fuse::FusePartInfo;
use common_storages_table_meta::meta::Compression;

use super::table::ParquetFileMeta;
use super::ParquetTable;

impl ParquetTable {
    #[inline]
    pub(super) fn do_read_partitions(
        &self,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let file_metas = self.read_file_metas()?;
        let arrow_schema = self.table_info.schema().to_arrow();
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        let (mut statistics, partitions) = match &push_down {
            None => self.all_columns_partitions(&file_metas, limit),
            Some(extras) => match &extras.projection {
                None => self.all_columns_partitions(&file_metas, limit),
                Some(projection) => {
                    self.projection_partitions(&file_metas, &column_leaves, projection, limit)
                }
            },
        };

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_down);
        // TODO: support prune parittions.
        statistics.partitions_scanned = file_metas.len();
        statistics.partitions_total = file_metas.len();

        Ok((statistics, partitions))
    }

    fn is_exact(push_downs: &Option<PushDownInfo>) -> bool {
        match push_downs {
            None => true,
            Some(extra) => extra.filters.is_empty(),
        }
    }

    fn all_columns_partitions(
        &self,
        file_metas: &[ParquetFileMeta],
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::create(PartitionsShuffleKind::Mod, vec![]);

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for meta in file_metas {
            let rows = meta.file_meta.num_rows;
            partitions.partitions.push(Self::all_columns_part(meta));
            statistics.read_rows += rows;
            statistics.read_bytes += meta
                .file_meta
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size())
                .sum::<usize>();

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }

        (statistics, partitions)
    }

    fn projection_partitions(
        &self,
        file_metas: &[ParquetFileMeta],
        column_leaves: &ColumnLeaves,
        projection: &Projection,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for meta in file_metas {
            let rows = meta.file_meta.num_rows;
            partitions
                .partitions
                .push(Self::projection_part(meta, column_leaves, projection));

            statistics.read_rows += rows;
            let columns = column_leaves.get_by_projection(projection).unwrap();
            for column in &columns {
                let indices = &column.leaf_ids;
                let col_metas = meta.file_meta.row_groups[0].columns();
                for index in indices {
                    let col_meta = col_metas[*index].metadata();
                    statistics.read_bytes += col_meta.total_compressed_size as usize;
                }
            }

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }
        (statistics, partitions)
    }

    fn all_columns_part(parquet_file_meta: &ParquetFileMeta) -> PartInfoPtr {
        let columns = parquet_file_meta.file_meta.row_groups[0].columns();
        let mut columns_meta = HashMap::with_capacity(columns.len());

        for (idx, column_meta) in columns.iter().enumerate() {
            let metadata = column_meta.metadata();
            let col_start = if let Some(dict_page_offset) = metadata.dictionary_page_offset {
                dict_page_offset
            } else {
                metadata.data_page_offset
            };
            columns_meta.insert(
                idx,
                ColumnMeta::create(
                    col_start as u64,
                    metadata.total_compressed_size as u64,
                    metadata.num_values as u64,
                ),
            );
        }

        FusePartInfo::create(
            parquet_file_meta.location.clone(),
            0,
            parquet_file_meta.file_meta.num_rows as u64,
            columns_meta,
            Compression::Lz4Raw, // only Lz4Raw is supported now (same as Fuse Engine)
        )
    }

    fn projection_part(
        parquet_file_meta: &ParquetFileMeta,
        column_leaves: &ColumnLeaves,
        projection: &Projection,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projection.len());
        let parquet_column_metas = parquet_file_meta.file_meta.row_groups[0].columns();

        let columns = column_leaves.get_by_projection(projection).unwrap();
        for column in &columns {
            let indices = &column.leaf_ids;
            for index in indices {
                let parquet_column_meta = &parquet_column_metas[*index];
                let metadata = parquet_column_meta.metadata();
                let col_start = if let Some(dict_page_offset) = metadata.dictionary_page_offset {
                    dict_page_offset
                } else {
                    metadata.data_page_offset
                };

                columns_meta.insert(
                    *index,
                    ColumnMeta::create(
                        col_start as u64,
                        metadata.total_compressed_size as u64,
                        metadata.num_values as u64,
                    ),
                );
            }
        }

        FusePartInfo::create(
            parquet_file_meta.location.clone(),
            0,
            parquet_file_meta.file_meta.num_rows as u64,
            columns_meta,
            Compression::Lz4Raw, // only Lz4Raw is supported now (same as Fuse Engine)
        )
    }
}
