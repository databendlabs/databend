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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Value;
use storages_common_index::filters::Filter;
use storages_common_index::filters::Xor8Filter;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::ColumnId;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use tracing::warn;

use crate::io::BlockFilterReader;
use crate::io::SegmentsIO;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_analyze(&self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        // 1. Read table snapshot.
        let r = self.read_table_snapshot().await;
        let snapshot_opt = match r {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                warn!(
                    "concurrent statistic: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc().await?,
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        if let Some(snapshot) = snapshot_opt {
            // 2. Iterator segments and blocks to estimate statistics.
            let mut filter_block_cols = vec![];
            for field in self.table_info.schema().fields() {
                filter_block_cols.push(BloomIndex::build_filter_column_name(field.name()));
            }
            let segments_io = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
            let segments = segments_io.read_segments(&snapshot.segments).await?;

            let mut column_distinct_count = HashMap::<ColumnId, u64>::new();
            for segment in segments {
                let segment = segment?;
                for block in &segment.blocks {
                    let block = block.as_ref().clone();
                    let index_location: Option<Location> = block.bloom_filter_index_location;
                    if let Some(loc) = index_location {
                        let row_count = block.row_count;
                        let index_len = block.bloom_filter_index_size;
                        if row_count != 0 {
                            let maybe_filter = loc
                                .read_filter(
                                    ctx.clone(),
                                    self.operator.clone(),
                                    &filter_block_cols,
                                    index_len,
                                )
                                .await;
                            match maybe_filter {
                                Ok(filter) => {
                                    let source_schema = filter.filter_schema;
                                    let block = filter.filter_block;
                                    for i in 0..block.num_columns() {
                                        let filter_bytes = match &block.get_by_offset(i).value {
                                            Value::Scalar(s) => s.as_string().unwrap(),
                                            Value::Column(c) => unsafe {
                                                c.as_string().unwrap().index_unchecked(0)
                                            },
                                        };
                                        let (filter, _size) = Xor8Filter::from_bytes(filter_bytes)?;
                                        if let Some(len) = filter.len() {
                                            let idx = source_schema
                                                .index_of(source_schema.field(i).name().as_str())
                                                .unwrap()
                                                as ColumnId;
                                            match column_distinct_count.get_mut(&idx) {
                                                Some(val) => {
                                                    *val += len as u64;
                                                }
                                                None => {
                                                    let _ = column_distinct_count
                                                        .insert(idx, len as u64);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "analyze table {:?} read bloom filter failed: {:?}",
                                        self.table_info.name, e
                                    );
                                    return Ok(());
                                }
                            }
                        }
                    } else {
                        warn!(
                            "table {:?} bloom_filter_index_location is None",
                            self.table_info.name
                        );
                        return Ok(());
                    }
                }
            }

            // 3. Generate new table statistics
            let table_statistics = TableSnapshotStatistics::new(column_distinct_count);
            let table_statistics_location = self
                .meta_location_generator
                .snapshot_statistics_location_from_uuid(
                    &table_statistics.snapshot_id,
                    table_statistics.format_version(),
                )?;

            // 4. Save table statistics
            let mut new_snapshot = TableSnapshot::from_previous(&snapshot);
            new_snapshot.table_statistics_location = Some(table_statistics_location);
            FuseTable::commit_to_meta_server(
                ctx.as_ref(),
                &self.table_info,
                &self.meta_location_generator,
                new_snapshot,
                Some(table_statistics),
                &self.operator,
            )
            .await?;
        }

        Ok(())
    }
}
