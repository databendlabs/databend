// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_storages_fuse::io::read::load_inverted_index_info;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::io::InvertedIndexWriter;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::ReadSettings;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::DEFAULT_ROW_PER_INDEX;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::IndexInfo;
use databend_storages_common_table_meta::meta::IndexSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;

pub struct Indexer {}

impl Indexer {
    pub(crate) fn new() -> Indexer {
        Indexer {}
    }

    #[async_backtrace::framed]
    pub(crate) async fn index(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<Option<String>> {
        let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
            // no snapshot
            return Ok(None);
        };
        if schema.fields.is_empty() {
            // no field for index
            return Ok(None);
        }

        let table_schema = &fuse_table.get_table_info().meta.schema;

        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        let projection = Projection::Columns(field_indices);
        let block_reader =
            fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

        let segment_reader =
            MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

        let index_info_loc = match &snapshot.index_info_locations {
            Some(locations) => locations.get(&index_name),
            None => None,
        };
        let old_index_info =
            load_inverted_index_info(fuse_table.get_operator(), index_info_loc).await?;

        let settings = ReadSettings::from_ctx(&ctx)?;
        let write_settings = fuse_table.get_write_settings();
        let storage_format = write_settings.storage_format;

        let operator = fuse_table.get_operator_ref();

        // If no segment locations are specified, iterates through all segments
        let segment_locs = if let Some(segment_locs) = segment_locs {
            segment_locs
        } else {
            snapshot.segments.clone()
        };

        let data_schema = DataSchema::from(schema.as_ref());

        // Grouping of segments, each group includes a number of segments to generate an index file.
        // Limit the index file size by check the sum row count to avoid too large index file.
        let mut sum_row_count = 0;
        let mut segments = Vec::new();
        let mut grouped_segments = Vec::new();
        for (segment_loc, ver) in segment_locs {
            // If segment has be indexed, we can ignore it.
            if let Some(ref old_index_info) = old_index_info {
                if old_index_info.indexed_segments.contains(&segment_loc) {
                    continue;
                }
            }
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: segment_loc.to_string(),
                    len_hint: None,
                    ver,
                    put_cache: false,
                })
                .await?;
            sum_row_count += segment_info.summary.row_count as usize;
            segments.push((segment_loc, segment_info));
            // Check sum of row count to prevent index getting too large.
            // TODO: suitable `DEFAULT_ROW_PER_INDEX` requires further testing.
            if sum_row_count > DEFAULT_ROW_PER_INDEX {
                let curr_segments = std::mem::take(&mut segments);
                grouped_segments.push(curr_segments);
                sum_row_count = 0;
            }
        }
        if !segments.is_empty() {
            grouped_segments.push(segments);
        }

        // All segments are already indexed and no need to do further operation.
        // TODO: Segments may require further regrouping to optimise the index data.
        if grouped_segments.is_empty() {
            return Ok(None);
        }
        let location_generator = fuse_table.meta_location_generator();

        // Generate index data for each group of segments.
        let mut indexes = BTreeMap::<String, Vec<IndexSegmentInfo>>::new();
        let mut indexed_segments = BTreeSet::<String>::new();
        for segments in grouped_segments {
            let mut index_segments = Vec::with_capacity(segments.len());
            let mut index_writer = InvertedIndexWriter::try_create(data_schema.clone())?;
            for (segment_loc, segment_info) in segments {
                let mut row_count = 0;
                let block_metas = segment_info.block_metas()?;
                for block_meta in block_metas {
                    let block = block_reader
                        .read_by_meta(&settings, &block_meta, &storage_format)
                        .await?;

                    index_writer.add_block(block)?;
                    row_count += block_meta.row_count;
                }
                indexed_segments.insert(segment_loc.to_string());
                let index_segment = IndexSegmentInfo {
                    segment_location: segment_loc.to_string(),
                    row_count,
                    block_range: None,
                };
                index_segments.push(index_segment);
            }

            let index_location = index_writer.finalize(operator, location_generator).await?;
            indexes.insert(index_location, index_segments);
        }

        // Merge old index info
        // TODO: remove unused index file
        if let Some(old_index_info) = old_index_info {
            for (index_loc, index_segments) in &old_index_info.indexes {
                indexes.insert(index_loc.to_string(), index_segments.to_vec());
            }
            for segment_loc in &old_index_info.indexed_segments {
                indexed_segments.insert(segment_loc.to_string());
            }
        }

        // Write new index info file
        let index_info = IndexInfo::new(schema, indexes, indexed_segments);
        let index_bytes = index_info.to_bytes()?;
        let new_index_info_loc = location_generator.gen_inverted_index_info_location();

        write_data(index_bytes, operator, &new_index_info_loc).await?;

        // Generate new table snapshot file
        let mut new_snapshot = TableSnapshot::from_previous(snapshot.as_ref());
        let mut index_info_locations = BTreeMap::new();
        if let Some(old_index_info_locations) = &snapshot.index_info_locations {
            for (old_index_name, location) in old_index_info_locations {
                if *old_index_name == index_name {
                    continue;
                }
                index_info_locations.insert(old_index_name.to_string(), location.clone());
            }
        }
        index_info_locations.insert(
            index_name.to_string(),
            (new_index_info_loc, IndexInfo::VERSION),
        );

        new_snapshot.index_info_locations = Some(index_info_locations);

        // Write new snapshot file
        let new_snapshot_location = location_generator
            .snapshot_location_from_uuid(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;

        let data = new_snapshot.to_bytes()?;
        fuse_table
            .get_operator_ref()
            .write(&new_snapshot_location, data)
            .await?;

        // Write new snapshot hint
        FuseTable::write_last_snapshot_hint(
            fuse_table.get_operator_ref(),
            fuse_table.meta_location_generator(),
            new_snapshot_location.clone(),
        )
        .await;

        Ok(Some(new_snapshot_location))
    }
}
