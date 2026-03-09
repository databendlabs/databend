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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use geo::Point;
use geo::Rect;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use opendal::Operator;

use crate::FuseBlockPartInfo;
use crate::io::read::SpatialIndexReader;

struct SpatialRuntimeFilter {
    column_id: ColumnId,
    srid: i32,
    rtrees: Arc<Vec<u8>>,
    rtree_bounds: Option<[f64; 4]>,
    stats: Arc<RuntimeFilterStats>,
}

pub struct SpatialRuntimePruner {
    filters: Vec<SpatialRuntimeFilter>,
    reader: SpatialIndexReader,
}

impl SpatialRuntimePruner {
    pub fn try_create(
        table_schema: TableSchemaRef,
        operator: Operator,
        settings: ReadSettings,
        entries: &[RuntimeFilterEntry],
    ) -> Result<Option<Self>> {
        let mut filters = Vec::new();
        for entry in entries {
            let Some(spatial) = entry.spatial.as_ref() else {
                continue;
            };
            if spatial.rtrees.is_empty() {
                continue;
            }
            let field = match table_schema.field_with_name(&spatial.column_name) {
                Ok(field) => field,
                Err(_) => continue,
            };
            filters.push(SpatialRuntimeFilter {
                column_id: field.column_id(),
                srid: spatial.srid,
                rtrees: spatial.rtrees.clone(),
                rtree_bounds: spatial.rtree_bounds,
                stats: entry.stats.clone(),
            });
        }

        if filters.is_empty() {
            return Ok(None);
        }

        let mut column_ids = Vec::new();
        let mut seen = HashSet::new();
        for filter in &filters {
            if seen.insert(filter.column_id) {
                column_ids.push(filter.column_id);
            }
        }
        let reader = SpatialIndexReader::create(operator.clone(), settings, column_ids);

        Ok(Some(Self { filters, reader }))
    }

    pub async fn prune(&self, part: &PartInfoPtr) -> Result<bool> {
        let part = FuseBlockPartInfo::from_part(part)?;
        let Some(spatial_stats) = part.spatial_stats.as_ref() else {
            return Ok(false);
        };
        if self.prune_by_stats(part, spatial_stats)? {
            return Ok(true);
        }
        let Some(location) = part.spatial_index_location.as_ref() else {
            return Ok(false);
        };

        let result = self.reader.read(&location.0).await?;
        let columns = result.columns;
        let column_id_to_index = result.column_id_to_index;

        for filter in &self.filters {
            let start = Instant::now();
            let Some(stat) = spatial_stats.get(&filter.column_id) else {
                continue;
            };
            if !stat.is_valid || stat.srid != filter.srid {
                continue;
            }
            let Some(column_index) = column_id_to_index.get(&filter.column_id) else {
                continue;
            };
            let Some(column) = columns.get(*column_index) else {
                continue;
            };
            let Some(ScalarRef::Binary(buffer)) = column.index(0) else {
                continue;
            };
            let query_tree = RTreeRef::<f64>::try_new(filter.rtrees.as_ref())
                .map_err(|e| ErrorCode::Internal(format!("Invalid runtime spatial filter: {e}")))?;
            let tree = RTreeRef::<f64>::try_new(&buffer)
                .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;

            let is_intersects = rtree_intersects_with_search(tree, query_tree)?;
            if !is_intersects {
                filter.stats.record_spatial(
                    start.elapsed().as_nanos() as u64,
                    part.nums_rows as u64,
                    1,
                );
                return Ok(true);
            }
            filter
                .stats
                .record_spatial(start.elapsed().as_nanos() as u64, 0, 0);
        }

        Ok(false)
    }

    fn prune_by_stats(
        &self,
        part: &FuseBlockPartInfo,
        spatial_stats: &HashMap<ColumnId, SpatialStatistics>,
    ) -> Result<bool> {
        for filter in &self.filters {
            let start = Instant::now();
            let Some(stat) = spatial_stats.get(&filter.column_id) else {
                continue;
            };
            if !stat.is_valid || stat.srid != filter.srid {
                continue;
            }
            let Some(bounds) = filter.rtree_bounds else {
                continue;
            };
            let query_rect = Rect::new(
                Point::new(bounds[0], bounds[1]),
                Point::new(bounds[2], bounds[3]),
            );
            let block_rect = Rect::new(
                Point::new(stat.min_x.into_inner(), stat.min_y.into_inner()),
                Point::new(stat.max_x.into_inner(), stat.max_y.into_inner()),
            );
            if !rects_intersect(&block_rect, &query_rect) {
                filter.stats.record_spatial(
                    start.elapsed().as_nanos() as u64,
                    part.nums_rows as u64,
                    1,
                );
                return Ok(true);
            }
            filter
                .stats
                .record_spatial(start.elapsed().as_nanos() as u64, 0, 0);
        }
        Ok(false)
    }
}

fn rects_intersect(block_rect: &Rect<f64>, query_rect: &Rect<f64>) -> bool {
    block_rect.min().x <= query_rect.max().x
        && block_rect.max().x >= query_rect.min().x
        && block_rect.min().y <= query_rect.max().y
        && block_rect.max().y >= query_rect.min().y
}

fn rtree_intersects_with_search(tree: RTreeRef<f64>, query_tree: RTreeRef<f64>) -> Result<bool> {
    if tree.num_items() == 0 || query_tree.num_items() == 0 {
        return Ok(false);
    }
    if query_tree.num_items() == 1 {
        let boxes = query_tree
            .boxes_at_level(0)
            .map_err(|e| ErrorCode::Internal(format!("Invalid runtime spatial filter: {e}")))?;
        if boxes.len() < 4 {
            return Ok(false);
        }
        for chunk in boxes.chunks_exact(4) {
            if !tree
                .search(chunk[0], chunk[1], chunk[2], chunk[3])
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    } else {
        let is_intersects = tree
            .intersection_candidates_with_other_tree(&query_tree)
            .next()
            .is_some();
        Ok(is_intersects)
    }
}
