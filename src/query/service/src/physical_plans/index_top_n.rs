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

use std::sync::Arc;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::types::F32;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::SourcePipeBuilder;
use databend_common_pipeline::sources::SyncSource;
use databend_common_pipeline::sources::SyncSourcer;
use databend_common_storages_fuse::FuseBlockPartInfo;

use super::EvalScalar;
use super::Filter;
use super::FinalTopNPlan;
use super::PartialTopNPlan;
use super::PhysicalPlanCast;
use super::TableScan;
use crate::pipelines::PipelineBuilder;
use crate::sessions::TableContextProgress;

/// Fuse the scan and both TopN stages only when pruning already supplied all
/// their input. Everything above FinalTopN (including RowFetch, masking and
/// result projection) stays in the original pipeline.
pub(super) fn try_build(plan: &FinalTopNPlan, builder: &mut PipelineBuilder) -> Result<bool> {
    let Some(input) = covering_input(plan) else {
        return Ok(false);
    };
    // This is a one-block source, not an alternative sort/spill implementation.
    let budget = builder.settings.get_max_block_size()? as usize;
    let mut rows = Vec::new();
    for part in &input.scan.source.parts.partitions {
        let Some(entries) = part
            .as_any()
            .downcast_ref::<FuseBlockPartInfo>()
            .and_then(|p| p.scored_rows(input.key))
        else {
            return Ok(false);
        };
        if entries.len() > budget - rows.len() {
            return Ok(false);
        }
        rows.extend(entries);
    }
    let progress = builder.ctx.get_scan_progress();
    let source = IndexTopNSource {
        rows: Some(rows),
        projection: input.projection,
        asc: plan.order_by[0].asc,
        offset: plan.offset,
        limit: plan.limit,
        partitions: input.scan.source.parts.len(),
        progress: progress.clone(),
    };
    let port = OutputPort::create();
    let mut pipe = SourcePipeBuilder::create();
    pipe.add_source(port.clone(), SyncSourcer::create(progress, port, source)?);
    builder.main_pipeline.add_pipe(pipe.finalize());
    Ok(true)
}

struct CoveringInput<'a> {
    scan: &'a TableScan,
    key: &'a InternalColumnType,
    projection: Vec<HitColumn>,
}

fn covering_input(plan: &FinalTopNPlan) -> Option<CoveringInput<'_>> {
    let [order] = plan.order_by.as_slice() else {
        return None;
    };
    let partial = PartialTopNPlan::from_physical_plan(&plan.input)?;
    if partial.order_by != plan.order_by
        || partial.candidate_count != plan.offset.checked_add(plan.limit)?
    {
        return None;
    }
    let mut input = &partial.input;
    if let Some(eval) = EvalScalar::from_physical_plan(input) {
        if !eval.exprs.is_empty() {
            return None;
        }
        input = &eval.input;
    }
    let filter = Filter::from_physical_plan(input);
    let leaf = filter.map_or(input, |f| &f.input);
    let scan = TableScan::from_physical_plan(leaf)?;
    let push = scan.source.push_downs.as_ref()?;
    if scan.source.parts.is_empty()
        || !matches!(&push.projection, Some(Projection::Columns(cols)) if cols.is_empty())
        || push.prewhere.is_some()
        || push.secure_filters.is_some()
        || push.sample.is_some()
        || push.change_type.is_some()
        || scan.source.block_meta_options.update_stream_columns
        || scan.source.block_meta_options.reserve_block_index
    {
        return None;
    }

    // Resolve binder symbols to actual internal columns; never trust a display name.
    let internal = scan.internal_column.as_ref()?;
    let kind = |field: &str| {
        internal
            .get(&field.parse::<usize>().ok()?)
            .map(|c| c.column_type())
    };
    let key = internal.get(&order.order_by.as_usize())?.column_type();
    match (key, filter) {
        (InternalColumnType::SearchScore, Some(f))
            if !f.is_secure && push.filter_only_use_index() && push.vector_index.is_none() =>
        {
            let [RemoteExpr::ColumnRef { id, .. }] = f.predicates.as_slice() else {
                return None;
            };
            let schema = leaf.output_schema().ok()?;
            if kind(schema.fields().get(*id)?.name())? != &InternalColumnType::SearchMatched {
                return None;
            }
        }
        (InternalColumnType::VectorScore, None)
            if push.vector_index.is_some()
                && push.inverted_index.is_none()
                && push.filters.is_none() => {}
        _ => return None,
    }
    let projection = partial
        .input
        .output_schema()
        .ok()?
        .fields()
        .iter()
        .map(|f| match kind(f.name())? {
            InternalColumnType::RowId => Some(HitColumn::RowId),
            t if t == key => Some(HitColumn::Score),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some(CoveringInput {
        scan,
        key,
        projection,
    })
}

enum HitColumn {
    RowId,
    Score,
}

struct IndexTopNSource {
    rows: Option<Vec<(F32, u64)>>,
    projection: Vec<HitColumn>,
    asc: bool,
    offset: usize,
    limit: usize,
    partitions: usize,
    progress: Arc<Progress>,
}

impl SyncSource for IndexTopNSource {
    const NAME: &'static str = "IndexTopNSource";
    const RECORD_SCAN_PROGRESS: bool = false;

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let Some(mut rows) = self.rows.take() else {
            return Ok(None);
        };
        self.progress.incr(&ProgressValues {
            rows: rows.len(),
            bytes: 0,
        });
        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, self.partitions);
        let cmp = |a: &(F32, u64), b: &(F32, u64)| if self.asc { a.cmp(b) } else { b.cmp(a) };
        let end = self.offset + self.limit;
        if end < rows.len() {
            rows.select_nth_unstable_by(end, cmp);
            rows.truncate(end);
        }
        rows.sort_unstable_by(cmp);
        let hits = &rows[self.offset.min(rows.len())..];
        if hits.is_empty() {
            // An OFFSET past the candidates: finish without a block, as a TopN would.
            return Ok(None);
        }
        let cols = self
            .projection
            .iter()
            .map(|col| match col {
                HitColumn::RowId => {
                    UInt64Type::from_data(hits.iter().map(|x| x.1).collect::<Vec<_>>()).into()
                }
                HitColumn::Score => {
                    Float32Type::from_data(hits.iter().map(|x| x.0).collect::<Vec<_>>()).into()
                }
            })
            .collect();
        Ok(Some(DataBlock::new(cols, hits.len())))
    }
}
