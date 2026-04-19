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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::IndexRuntimeFilter;
use databend_common_catalog::runtime_filter_info::PartitionRuntimeFilter;
use databend_common_catalog::runtime_filter_info::RowRuntimeFilter;
use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::Bitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_io::ReadSettings;
use geo::Point;
use geo::Rect;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use opendal::Operator;

use crate::FuseBlockPartInfo;
use crate::index::rects_intersect;
use crate::io::read::SpatialIndexReadResult;
use crate::io::read::SpatialIndexReader;
use crate::pruning::ExprBloomFilter;
use crate::pruning::bloom_pruner::should_prune_runtime_inlist_by_bloom_index;

// --- MinMaxPartitionFilter ---

pub struct MinMaxPartitionFilter {
    func_ctx: FunctionContext,
    table_schema: TableSchemaRef,
    expr: Expr<String>,
}

impl MinMaxPartitionFilter {
    pub fn new(
        func_ctx: FunctionContext,
        table_schema: TableSchemaRef,
        expr: Expr<String>,
    ) -> Self {
        Self {
            func_ctx,
            table_schema,
            expr,
        }
    }
}

impl PartitionRuntimeFilter for MinMaxPartitionFilter {
    fn prune(&self, part: &PartInfoPtr) -> bool {
        let Ok(part) = FuseBlockPartInfo::from_part(part) else {
            return false;
        };
        if matches!(
            &self.expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ) {
            return true;
        }
        let column_refs = self.expr.column_refs();
        let ty = match column_refs.values().last() {
            Some(ty) => ty,
            None => return false,
        };
        let name = match column_refs.keys().last() {
            Some(name) => name,
            None => return false,
        };
        let Some(stats) = &part.columns_stat else {
            return false;
        };
        let column_ids = self.table_schema.leaf_columns_of(name);
        if column_ids.len() != 1 {
            return false;
        }
        let Some(stat) = stats.get(&column_ids[0]) else {
            return false;
        };
        let domain = statistics_to_domain(vec![stat], ty);
        let mut input_domains = HashMap::new();
        input_domains.insert(name.to_string(), domain);
        let (new_expr, _) = ConstantFolder::fold_with_domain(
            &self.expr,
            &input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );
        matches!(
            new_expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        )
    }
}

// --- InlistBloomIndexFilter ---

pub struct InlistBloomIndexFilter {
    func_ctx: FunctionContext,
    table_schema: TableSchemaRef,
    settings: ReadSettings,
    expr: Expr<String>,
    inlist_value_count: usize,
    inlist_bloom_prune_threshold: usize,
}

impl InlistBloomIndexFilter {
    pub fn new(
        func_ctx: FunctionContext,
        table_schema: TableSchemaRef,
        settings: ReadSettings,
        expr: Expr<String>,
        inlist_value_count: usize,
        inlist_bloom_prune_threshold: usize,
    ) -> Self {
        Self {
            func_ctx,
            table_schema,
            settings,
            expr,
            inlist_value_count,
            inlist_bloom_prune_threshold,
        }
    }
}

// PLACEHOLDER_APPEND2

#[async_trait::async_trait]
impl IndexRuntimeFilter for InlistBloomIndexFilter {
    async fn load_index(
        &self,
        part: &PartInfoPtr,
        operator: &Operator,
    ) -> Result<Option<Box<dyn Any + Send>>> {
        if self.inlist_value_count == 0
            || self.inlist_value_count > self.inlist_bloom_prune_threshold
        {
            return Ok(None);
        }
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let pruned = should_prune_runtime_inlist_by_bloom_index(
            &self.func_ctx,
            operator,
            &self.settings,
            &self.table_schema,
            &self.expr,
            fuse_part,
        )
        .await?;
        Ok(Some(Box::new(pruned)))
    }

    fn prune(&self, _part: &PartInfoPtr, index: Option<&dyn Any>) -> Result<bool> {
        if let Some(result) = index.and_then(|i| i.downcast_ref::<bool>()) {
            return Ok(*result);
        }
        Ok(false)
    }
}

// --- SpatialIndexFilter ---

pub struct SpatialIndexFilter {
    column_id: ColumnId,
    srid: i32,
    rtrees: Arc<Vec<u8>>,
    rtree_bounds: Option<[f64; 4]>,
    settings: ReadSettings,
    column_ids: Vec<ColumnId>,
}

impl SpatialIndexFilter {
    pub fn new(
        column_id: ColumnId,
        srid: i32,
        rtrees: Arc<Vec<u8>>,
        rtree_bounds: Option<[f64; 4]>,
        settings: ReadSettings,
    ) -> Self {
        Self {
            column_ids: vec![column_id],
            column_id,
            srid,
            rtrees,
            rtree_bounds,
            settings,
        }
    }
}

// PLACEHOLDER_APPEND3

#[async_trait::async_trait]
impl IndexRuntimeFilter for SpatialIndexFilter {
    async fn load_index(
        &self,
        part: &PartInfoPtr,
        operator: &Operator,
    ) -> Result<Option<Box<dyn Any + Send>>> {
        let part = FuseBlockPartInfo::from_part(part)?;

        // Fast path: check bounding box stats before any IO
        if let Some(spatial_stats) = part.spatial_stats.as_ref() {
            if let Some(stat) = spatial_stats.get(&self.column_id) {
                if !stat.is_valid || stat.srid != self.srid {
                    return Ok(None);
                }
                if let Some(bounds) = self.rtree_bounds {
                    let query_rect = Some(Rect::new(
                        Point::new(bounds[0], bounds[1]),
                        Point::new(bounds[2], bounds[3]),
                    ));
                    let block_rect = Rect::new(
                        Point::new(stat.min_x.into_inner(), stat.min_y.into_inner()),
                        Point::new(stat.max_x.into_inner(), stat.max_y.into_inner()),
                    );
                    if !rects_intersect(&block_rect, &query_rect) {
                        // Pruned by bounding box — no need to load index
                        return Ok(Some(Box::new(true)));
                    }
                }
            }
        }

        let Some(location) = part.spatial_index_location.as_ref() else {
            return Ok(None);
        };
        let reader =
            SpatialIndexReader::create(operator.clone(), self.settings, self.column_ids.clone());
        let result = reader.read(&location.0).await?;
        Ok(Some(Box::new(result)))
    }

    fn prune(&self, _part: &PartInfoPtr, index: Option<&dyn Any>) -> Result<bool> {
        let Some(index) = index else {
            return Ok(false);
        };
        // Fast path: already pruned by bounding box in load_index
        if let Some(&pruned) = index.downcast_ref::<bool>() {
            return Ok(pruned);
        }
        // Full path: check rtree intersection from loaded index
        let Some(result) = index.downcast_ref::<SpatialIndexReadResult>() else {
            return Ok(false);
        };
        let Some(col_idx) = result.column_id_to_index.get(&self.column_id) else {
            return Ok(false);
        };
        let Some(column) = result.columns.get(*col_idx) else {
            return Ok(false);
        };
        let Some(ScalarRef::Binary(buffer)) = column.index(0) else {
            return Ok(false);
        };
        let query_tree = RTreeRef::<f64>::try_new(self.rtrees.as_ref())
            .map_err(|e| ErrorCode::Internal(format!("Invalid runtime spatial filter: {e}")))?;
        let tree = RTreeRef::<f64>::try_new(&buffer)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index: {e}")))?;

        let is_intersects = rtree_intersects_with_search(tree, query_tree)?;
        Ok(!is_intersects)
    }
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
        Ok(tree
            .intersection_candidates_with_other_tree(&query_tree)
            .next()
            .is_some())
    }
}

// --- BloomRowFilter ---

pub struct BloomRowFilter {
    column_name: String,
    filter: Arc<Sbbf>,
}

impl BloomRowFilter {
    pub fn create(column_name: String, filter: Arc<Sbbf>) -> Arc<dyn RowRuntimeFilter> {
        Arc::new(Self { column_name, filter })
    }
}

impl RowRuntimeFilter for BloomRowFilter {
    fn column_name(&self) -> &str {
        &self.column_name
    }

    fn apply(&self, column: Column) -> Result<Bitmap> {
        let bitmap = ExprBloomFilter::new(&self.filter).apply(column)?;
        Ok(bitmap.into())
    }
}
