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
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_io::UNKNOWN_SRID;
use databend_common_io::geometry::ewkb_to_bbox;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use geo_index::rtree::sort::HilbertSort;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::pipelines::executor::WatchNotify;

#[derive(Clone, Copy, Debug)]
struct SpatialBbox {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

/// Returns the bbox and SRID for a geometry value at `row`.
/// `None` means NULL or non-geometry; `Some((None, srid))` means an empty geometry.
fn extract_geometry_bbox_and_srid(
    value: &Value<AnyType>,
    row: usize,
) -> Result<Option<(Option<SpatialBbox>, i32)>> {
    let scalar = match value {
        Value::Scalar(scalar) => scalar.as_ref(),
        Value::Column(column) => match column.index(row) {
            Some(scalar) => scalar,
            None => return Ok(None),
        },
    };

    let ScalarRef::Geometry(ewkb) = scalar else {
        return Ok(None);
    };

    let ewkb_bbox = ewkb_to_bbox(ewkb).ok_or_else(|| {
        ErrorCode::GeometryError("Failed to parse geometry EWKB in spatial index join")
    })?;
    let srid = ewkb_bbox.srid.unwrap_or(UNKNOWN_SRID);
    let bbox = ewkb_bbox.bbox.map(|bbox| {
        let (min_x, min_y, max_x, max_y) = bbox.corners();
        SpatialBbox {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    });
    Ok(Some((bbox, srid)))
}

/// Checks probe SRID against build-side SRIDs before bbox filtering can skip a pair.
fn check_probe_srid_compatible(
    probe_srid: i32,
    build_srids: &BTreeSet<i32>,
    build_side: SpatialBuildSide,
) -> Result<()> {
    for build_srid in build_srids {
        if *build_srid != probe_srid {
            let (left_srid, right_srid) = match build_side {
                SpatialBuildSide::Left => (*build_srid, probe_srid),
                SpatialBuildSide::Right => (probe_srid, *build_srid),
            };
            return Err(ErrorCode::GeometryError(format!(
                "Incompatible SRID: {} and {}",
                left_srid, right_srid
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum SpatialBuildSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug)]
struct BuildRowLocation {
    block_index: usize,
    row_index: usize,
}

/// A build row that has a bbox and is therefore indexed in the R-tree. The
/// R-tree is built from these in order, so a search result index maps straight
/// back to `indexed_rows[index]`.
#[derive(Clone, Copy, Debug)]
struct IndexedBuildRow {
    bbox: SpatialBbox,
    location: BuildRowLocation,
}

#[derive(Default)]
struct SpatialIndexBuildData {
    blocks: Vec<DataBlock>,
    indexed_rows: Vec<IndexedBuildRow>,
    // Empty geometries have no bbox to index, but their SRID still matters.
    srids: BTreeSet<i32>,
}

pub struct SpatialIndexJoinState {
    function_context: FunctionContext,
    build_geometry: RemoteExpr,
    probe_geometry: RemoteExpr,
    predicates: Vec<RemoteExpr>,
    output_projection: BTreeSet<usize>,
    build_side: SpatialBuildSide,
    output_compact_rows: usize,
    local_build_blocks: Mutex<Vec<DataBlock>>,
    build_data: RwLock<SpatialIndexBuildData>,
    rtree_bytes: RwLock<Vec<u8>>,
    build_sinker_count: Mutex<usize>,
    build_finished: Mutex<bool>,
    finished_notify: Arc<WatchNotify>,
}

impl SpatialIndexJoinState {
    pub fn create(
        function_context: FunctionContext,
        build_geometry: RemoteExpr,
        probe_geometry: RemoteExpr,
        predicates: Vec<RemoteExpr>,
        output_projection: Vec<usize>,
        build_side: SpatialBuildSide,
        output_compact_rows: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            function_context,
            build_geometry,
            probe_geometry,
            predicates,
            output_projection: output_projection.into_iter().collect(),
            build_side,
            output_compact_rows: output_compact_rows.max(1),
            local_build_blocks: Mutex::new(Vec::new()),
            build_data: RwLock::new(SpatialIndexBuildData::default()),
            rtree_bytes: RwLock::new(Vec::new()),
            build_sinker_count: Mutex::new(0),
            build_finished: Mutex::new(false),
            finished_notify: Arc::new(WatchNotify::new()),
        })
    }

    pub fn build_attach(&self) {
        *self.build_sinker_count.lock() += 1;
    }

    pub fn sink_local_build(&self, block: DataBlock) {
        self.local_build_blocks.lock().push(block);
    }

    pub fn finish_local_build(&self) -> Result<()> {
        let local_blocks = std::mem::take(&mut *self.local_build_blocks.lock());
        for block in local_blocks {
            self.ingest_build_block(block)?;
        }
        Ok(())
    }

    fn ingest_build_block(&self, block: DataBlock) -> Result<()> {
        let evaluator = Evaluator::new(&block, &self.function_context, &BUILTIN_FUNCTIONS);
        let geometry = evaluator.run(&self.build_geometry.as_expr(&BUILTIN_FUNCTIONS))?;

        let mut build_data = self.build_data.write();
        let block_index = build_data.blocks.len();

        for row in 0..block.num_rows() {
            let Some((bbox, srid)) = extract_geometry_bbox_and_srid(&geometry, row)? else {
                continue;
            };
            build_data.srids.insert(srid);
            let Some(bbox) = bbox else {
                continue;
            };
            build_data.indexed_rows.push(IndexedBuildRow {
                bbox,
                location: BuildRowLocation {
                    block_index,
                    row_index: row,
                },
            });
        }

        build_data.blocks.push(block);
        Ok(())
    }

    pub fn build_detach_and_finish(&self) -> Result<()> {
        let mut count = self.build_sinker_count.lock();
        *count -= 1;
        if *count == 0 {
            drop(count);
            self.finish_build()?;
            *self.build_finished.lock() = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    fn finish_build(&self) -> Result<()> {
        let build_data = self.build_data.read();
        let num_items = u32::try_from(build_data.indexed_rows.len()).map_err(|_| {
            ErrorCode::Internal("Spatial index join build side is too large".to_string())
        })?;
        let mut builder = RTreeBuilder::<f64>::new(num_items);
        for row in build_data.indexed_rows.iter() {
            let bbox = &row.bbox;
            builder.add(bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y);
        }
        *self.rtree_bytes.write() = builder.finish::<HilbertSort>().into_inner();
        Ok(())
    }

    pub async fn wait_build_finish(&self) {
        let notified = {
            let build_finished = self.build_finished.lock();
            if *build_finished {
                None
            } else {
                Some(self.finished_notify.notified())
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }
    }

    pub fn probe_block(&self, probe: DataBlock) -> Result<Vec<DataBlock>> {
        // The build side is read-only once the build phase finishes, so we hold a
        // single read guard for the whole probe instead of re-locking per row.
        let build_data = self.build_data.read();
        if build_data.srids.is_empty() || probe.is_empty() {
            return Ok(vec![]);
        }

        let evaluator = Evaluator::new(&probe, &self.function_context, &BUILTIN_FUNCTIONS);
        let probe_geometry = evaluator.run(&self.probe_geometry.as_expr(&BUILTIN_FUNCTIONS))?;

        let rtree_bytes = self.rtree_bytes.read();
        let rtree = RTreeRef::<f64>::try_new(&*rtree_bytes)
            .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index R-tree: {e}")))?;

        let mut output = Vec::new();
        for probe_row in 0..probe.num_rows() {
            output.extend(self.probe_one_row(
                &build_data,
                &rtree,
                &probe,
                &probe_geometry,
                probe_row,
            )?);
        }
        compact_output_blocks(output, self.output_compact_rows)
    }

    fn probe_one_row(
        &self,
        build_data: &SpatialIndexBuildData,
        rtree: &RTreeRef<f64>,
        probe: &DataBlock,
        probe_geometry: &Value<AnyType>,
        probe_row: usize,
    ) -> Result<Vec<DataBlock>> {
        let Some((probe_bbox, probe_srid)) =
            extract_geometry_bbox_and_srid(probe_geometry, probe_row)?
        else {
            return Ok(vec![]);
        };
        // SRID check runs before any bbox-miss skip so an incompatible-SRID pair
        // raises the same error as the fallback join instead of being dropped.
        check_probe_srid_compatible(probe_srid, &build_data.srids, self.build_side)?;

        let Some(probe_bbox) = probe_bbox else {
            return Ok(vec![]);
        };
        let candidate_indexes = rtree.search(
            probe_bbox.min_x,
            probe_bbox.min_y,
            probe_bbox.max_x,
            probe_bbox.max_y,
        );

        // The R-tree was built from `indexed_rows`, so every search result indexes
        // into it. Group candidates by build block to take whole runs at once.
        let mut build_rows_by_block: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        for candidate_index in candidate_indexes {
            let location = build_data.indexed_rows[candidate_index as usize].location;
            build_rows_by_block
                .entry(location.block_index)
                .or_default()
                .push(location.row_index as u32);
        }

        let mut output = Vec::with_capacity(build_rows_by_block.len());
        for (build_block_index, build_indices) in build_rows_by_block {
            let candidate = self.build_candidate_block(
                build_data,
                probe,
                probe_row,
                build_block_index,
                &build_indices,
            )?;
            let filtered = self.filter_predicates(candidate)?;
            if !filtered.is_empty() {
                output.push(filtered.project(&self.output_projection));
            }
        }
        Ok(output)
    }

    fn build_candidate_block(
        &self,
        build_data: &SpatialIndexBuildData,
        probe: &DataBlock,
        probe_row: usize,
        build_block_index: usize,
        build_indices: &[u32],
    ) -> Result<DataBlock> {
        // `build_block_index` is an `indexed_rows` location, so it always indexes
        // into `build_data.blocks`.
        let build_taken =
            build_data.blocks[build_block_index].take_with_optimize_size(build_indices)?;
        let rows = build_indices.len();

        // Build and probe columns are concatenated in left-then-right order; only
        // which side is the indexed (build) one differs.
        let build_columns = build_taken.columns().iter().cloned();
        let probe_columns = const_probe_columns(probe, probe_row, rows);
        let columns: Vec<BlockEntry> = match self.build_side {
            SpatialBuildSide::Left => build_columns.chain(probe_columns).collect(),
            SpatialBuildSide::Right => probe_columns.chain(build_columns).collect(),
        };

        Ok(DataBlock::new(columns, rows))
    }

    fn filter_predicates(&self, mut block: DataBlock) -> Result<DataBlock> {
        for predicate in &self.predicates {
            if block.is_empty() {
                return Ok(block);
            }
            block = filter_block(block, predicate, &self.function_context)?;
        }
        Ok(block)
    }
}

fn const_probe_columns(
    probe: &DataBlock,
    probe_row: usize,
    rows: usize,
) -> impl Iterator<Item = BlockEntry> + '_ {
    // `probe_row` is a valid row index into `probe`, so indexing every column at
    // that row always succeeds.
    probe.columns().iter().map(move |entry| {
        BlockEntry::Const(
            entry.index(probe_row).unwrap().to_owned(),
            entry.data_type(),
            rows,
        )
    })
}

fn filter_block(
    block: DataBlock,
    filter: &RemoteExpr,
    func_ctx: &FunctionContext,
) -> Result<DataBlock> {
    let filter = filter.as_expr(&BUILTIN_FUNCTIONS);
    let predicate = cast_expr_to_non_null_boolean(filter)?;
    debug_assert_eq!(predicate.data_type(), &DataType::Boolean);

    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let predicate = evaluator
        .run(&predicate)?
        .try_downcast::<databend_common_expression::types::BooleanType>()
        .unwrap();
    block.filter_boolean_value(&predicate)
}

fn compact_output_blocks(blocks: Vec<DataBlock>, compact_rows: usize) -> Result<Vec<DataBlock>> {
    let mut compacted_blocks = Vec::new();
    let mut current_blocks = Vec::new();
    let mut current_rows = 0;
    let compact_rows = compact_rows.max(1);

    for block in blocks {
        let block_rows = block.num_rows();

        if current_rows + block_rows >= compact_rows {
            if !current_blocks.is_empty() {
                let compacted = DataBlock::concat(&current_blocks)?;
                if !compacted.is_empty() {
                    compacted_blocks.push(compacted);
                }
                current_blocks.clear();
                current_rows = 0;
            }

            if block_rows >= compact_rows {
                compacted_blocks.push(block);
            } else {
                current_rows = block_rows;
                current_blocks.push(block);
            }
        } else {
            current_rows += block_rows;
            current_blocks.push(block);
        }
    }

    if !current_blocks.is_empty() {
        let compacted = DataBlock::concat(&current_blocks)?;
        if !compacted.is_empty() {
            compacted_blocks.push(compacted);
        }
    }

    Ok(compacted_blocks)
}

enum SpatialIndexJoinStep {
    WaitBuild,
    Probe,
}

pub struct TransformSpatialIndexJoinProbe {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: VecDeque<DataBlock>,
    state: Arc<SpatialIndexJoinState>,
    step: SpatialIndexJoinStep,
}

impl TransformSpatialIndexJoinProbe {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        state: Arc<SpatialIndexJoinState>,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            input_port,
            output_port,
            input_data: None,
            output_data: VecDeque::new(),
            state,
            step: SpatialIndexJoinStep::WaitBuild,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformSpatialIndexJoinProbe {
    fn name(&self) -> String {
        "TransformSpatialIndexJoinProbe".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            SpatialIndexJoinStep::WaitBuild => Ok(Event::Async),
            SpatialIndexJoinStep::Probe => {
                if self.output_port.is_finished() {
                    return Ok(Event::Finished);
                }
                if !self.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }
                if let Some(block) = self.output_data.pop_front() {
                    self.output_port.push_data(Ok(block));
                    return Ok(Event::NeedConsume);
                }
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }
                if self.input_port.is_finished() {
                    self.output_port.finish();
                    return Ok(Event::Finished);
                }
                if self.input_port.has_data() {
                    self.input_data = Some(self.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                } else {
                    self.input_port.set_need_data();
                    Ok(Event::NeedData)
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            self.output_data.extend(self.state.probe_block(block)?);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let SpatialIndexJoinStep::WaitBuild = self.step {
            self.state.wait_build_finish().await;
            self.step = SpatialIndexJoinStep::Probe;
        }
        Ok(())
    }
}

pub struct TransformSpatialIndexJoinBuild {
    state: Arc<SpatialIndexJoinState>,
}

impl TransformSpatialIndexJoinBuild {
    pub fn create(state: Arc<SpatialIndexJoinState>) -> Self {
        state.build_attach();
        Self { state }
    }
}

#[async_trait::async_trait]
impl AsyncSink for TransformSpatialIndexJoinBuild {
    const NAME: &'static str = "TransformSpatialIndexJoinBuild";

    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        self.state.sink_local_build(data_block);
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        self.state.finish_local_build()?;
        self.state.build_detach_and_finish()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::GeometryType;
    use databend_common_io::geometry::geometry_from_ewkt;

    use super::*;

    #[test]
    fn test_spatial_index_join_uses_rtree_candidates() -> Result<()> {
        let build_block = DataBlock::new_from_columns(vec![GeometryType::from_data(vec![
            geometry_from_ewkt("POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))", None)?,
            geometry_from_ewkt("POLYGON((20 20, 25 20, 25 25, 20 25, 20 20))", None)?,
        ])]);
        let probe_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT(1 1)",
                None,
            )?])]);

        let build_geometry: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "build_geom".to_string(),
        }
        .into();
        let probe_geometry: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "probe_geom".to_string(),
        }
        .into();

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            build_geometry.as_remote_expr(),
            probe_geometry.as_remote_expr(),
            vec![],
            vec![0, 1],
            SpatialBuildSide::Right,
            8192,
        );
        state.build_attach();
        state.sink_local_build(build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let output = state.probe_block(probe_block)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1);
        assert_eq!(output[0].num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_spatial_index_join_compacts_probe_output_blocks() -> Result<()> {
        let first_build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))",
                None,
            )?])]);
        let second_build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POLYGON((20 20, 25 20, 25 25, 20 25, 20 20))",
                None,
            )?])]);
        let probe_block = DataBlock::new_from_columns(vec![GeometryType::from_data(vec![
            geometry_from_ewkt("POINT(1 1)", None)?,
            geometry_from_ewkt("POINT(21 21)", None)?,
        ])]);

        let build_geometry: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "build_geom".to_string(),
        }
        .into();
        let probe_geometry: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "probe_geom".to_string(),
        }
        .into();

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            build_geometry.as_remote_expr(),
            probe_geometry.as_remote_expr(),
            vec![],
            vec![0, 1],
            SpatialBuildSide::Right,
            8192,
        );
        state.build_attach();
        state.sink_local_build(first_build_block);
        state.sink_local_build(second_build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let output = state.probe_block(probe_block)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(output[0].num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_spatial_index_join_filters_bbox_false_positives() -> Result<()> {
        // The bbox overlaps, but ST_Intersects must reject the candidate.
        let build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POLYGON((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))",
                None,
            )?])]);
        let probe_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT(7 7)",
                None,
            )?])]);

        let build_col: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "build_geom".to_string(),
        }
        .into();
        let probe_col: databend_common_expression::Expr = ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: "probe_geom".to_string(),
        }
        .into();

        use databend_common_expression::type_check::check_function;
        let st_intersects = check_function(
            None,
            "st_intersects",
            &[],
            &[
                ColumnRef {
                    span: None,
                    id: 0usize,
                    data_type: DataType::Geometry,
                    display_name: "probe_geom".to_string(),
                }
                .into(),
                ColumnRef {
                    span: None,
                    id: 1usize,
                    data_type: DataType::Geometry,
                    display_name: "build_geom".to_string(),
                }
                .into(),
            ],
            &BUILTIN_FUNCTIONS,
        )?;

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            build_col.as_remote_expr(),
            probe_col.as_remote_expr(),
            vec![st_intersects.as_remote_expr()],
            vec![0, 1],
            SpatialBuildSide::Right,
            8192,
        );
        state.build_attach();
        state.sink_local_build(build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let output = state.probe_block(probe_block)?;
        assert!(
            output.is_empty() || output.iter().all(|b| b.num_rows() == 0),
            "expected no output rows for bbox false positive, got: {output:?}"
        );
        Ok(())
    }

    fn geom_column_ref(display_name: &str) -> databend_common_expression::Expr {
        ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Geometry,
            display_name: display_name.to_string(),
        }
        .into()
    }

    #[test]
    fn test_spatial_index_join_raises_incompatible_srid_on_bbox_miss() -> Result<()> {
        let build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))",
                Some(3857),
            )?])]);
        let probe_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT(100 100)",
                Some(4326),
            )?])]);

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            geom_column_ref("build_geom").as_remote_expr(),
            geom_column_ref("probe_geom").as_remote_expr(),
            vec![],
            vec![0, 1],
            SpatialBuildSide::Right,
            8192,
        );
        state.build_attach();
        state.sink_local_build(build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let err = state
            .probe_block(probe_block)
            .expect_err("incompatible SRID must raise an error even on a bbox miss");
        assert_eq!(
            err.message(),
            "Incompatible SRID: 4326 and 3857",
            "expected fallback-aligned SRID order, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_spatial_index_join_empty_build_geometry_keeps_srid_check() -> Result<()> {
        let build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT EMPTY",
                Some(3857),
            )?])]);
        let probe_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT(1 1)",
                Some(4326),
            )?])]);

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            geom_column_ref("build_geom").as_remote_expr(),
            geom_column_ref("probe_geom").as_remote_expr(),
            vec![],
            vec![0, 1],
            SpatialBuildSide::Right,
            8192,
        );
        state.build_attach();
        state.sink_local_build(build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let err = state
            .probe_block(probe_block)
            .expect_err("empty build geometry must still enforce SRID compatibility");
        assert!(
            err.message().contains("Incompatible SRID"),
            "expected Incompatible SRID error, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_spatial_index_join_srid_error_order_for_left_build() -> Result<()> {
        let build_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))",
                Some(3857),
            )?])]);
        let probe_block =
            DataBlock::new_from_columns(vec![GeometryType::from_data(vec![geometry_from_ewkt(
                "POINT(100 100)",
                Some(4326),
            )?])]);

        let state = SpatialIndexJoinState::create(
            FunctionContext::default(),
            geom_column_ref("build_geom").as_remote_expr(),
            geom_column_ref("probe_geom").as_remote_expr(),
            vec![],
            vec![0, 1],
            SpatialBuildSide::Left,
            8192,
        );
        state.build_attach();
        state.sink_local_build(build_block);
        state.finish_local_build()?;
        state.build_detach_and_finish()?;

        let err = state
            .probe_block(probe_block)
            .expect_err("incompatible SRID must raise an error even on a bbox miss");
        assert_eq!(
            err.message(),
            "Incompatible SRID: 3857 and 4326",
            "expected fallback-aligned SRID order for left build, got: {err}"
        );
        Ok(())
    }
}
