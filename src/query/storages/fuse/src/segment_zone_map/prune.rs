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

use databend_common_base::runtime::spawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_pruner::RangeIndexInput;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_pruner::RangePrunerCreator;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentZoneMapMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use tokio::sync::Semaphore;

use crate::segment_zone_map::format::SegmentZoneMapFile;
use crate::segment_zone_map::format::SegmentZoneMapReader;
use crate::segment_zone_map::format::ZoneMapColumn;

type SharedRangePruner = Arc<dyn RangePruner + Send + Sync>;

/// Drop the segments that the zone map proves cannot match `filter`.
///
/// `segments` pairs each segment with its position in the snapshot; positions
/// are preserved. Segments the zone map does not cover are kept, and the kept
/// segments still go through regular segment pruning, so the final pruning
/// result is the same as without the zone map.
///
/// Row groups are read and evaluated on their own tasks, `concurrency` at a
/// time, so memory is bounded by that many row groups rather than the file.
pub async fn prune_segments_by_zone_map(
    reader: &SegmentZoneMapReader,
    meta: &SegmentZoneMapMeta,
    func_ctx: FunctionContext,
    schema: &TableSchemaRef,
    filter: &Expr<String>,
    segments: &[(usize, Location)],
    concurrency: usize,
) -> Result<Vec<(usize, Location)>> {
    let file = reader.open(meta).await?;
    let column_ids = usable_column_ids(file.columns(), schema, filter);
    if column_ids.is_empty() {
        return Ok(segments.to_vec());
    }
    let range_pruner = RangePrunerCreator::try_create(func_ctx, schema, Some(filter))?;
    let group_stats = file.row_group_stats(&column_ids);
    let (file, column_ids) = (Arc::new(file), Arc::new(column_ids));
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));

    let mut tasks = Vec::with_capacity(group_stats.len());
    for (group, stats) in group_stats.into_iter().enumerate() {
        // A group whose bounds already fail the filter has every row dropped
        // without reading its statistics.
        let whole = !range_pruner.should_keep(&RangeIndexInput::from_columns(&stats), None);
        let (file, column_ids) = (file.clone(), column_ids.clone());
        let (range_pruner, semaphore) = (range_pruner.clone(), semaphore.clone());
        tasks.push(spawn(async move {
            let _permit = semaphore.acquire().await.map_err(|_| closed())?;
            dropped_paths(&file, group, &column_ids, whole, &range_pruner).await
        }));
    }
    let mut dropped = HashSet::new();
    for task in tasks {
        dropped.extend(task.await??);
    }
    let kept = segments
        .iter()
        .filter(|(_, location)| !dropped.contains(&location.0));
    Ok(kept.cloned().collect())
}

fn closed() -> ErrorCode {
    ErrorCode::Internal("segment zone map pruning semaphore closed")
}

/// Paths of the rows of row group `group` that cannot match. With `whole`, the
/// group's bounds already failed the filter: only the paths are read.
async fn dropped_paths(
    file: &SegmentZoneMapFile,
    group: usize,
    column_ids: &HashSet<ColumnId>,
    whole: bool,
    range_pruner: &SharedRangePruner,
) -> Result<Vec<String>> {
    let none = HashSet::new();
    let data = file
        .read_row_group(group, if whole { &none } else { column_ids })
        .await?;
    let mut dropped = Vec::new();
    let mut stats = StatisticsOfColumns::with_capacity(column_ids.len());
    for row in 0..data.num_rows() {
        if !whole {
            data.fill_stats(row, &mut stats);
            if range_pruner.should_keep(&RangeIndexInput::from_columns(&stats), None) {
                continue;
            }
        }
        dropped.push(data.path(row).to_string());
    }
    Ok(dropped)
}

/// Covered columns that `filter` references and whose stored type is still the
/// column's type. A column whose type changed since the zone map was built (a
/// metadata-only change keeps the segment paths) must not be used: its stored
/// min/max would be compared under the wrong type.
fn usable_column_ids(
    covered: &[ZoneMapColumn],
    schema: &TableSchemaRef,
    filter: &Expr<String>,
) -> HashSet<ColumnId> {
    let mut referenced = HashSet::new();
    for name in filter.column_refs().into_keys() {
        if let Ok(field) = schema.field_with_name(&name) {
            referenced.extend(field.leaf_column_ids());
        }
    }
    let mut leaf_types = HashMap::new();
    for field in schema.leaf_fields() {
        leaf_types.insert(field.column_id(), field.data_type().clone());
    }
    let mut usable = HashSet::new();
    for column in covered {
        if referenced.contains(&column.column_id)
            && leaf_types.get(&column.column_id) == Some(&column.data_type)
        {
            usable.insert(column.column_id);
        }
    }
    usable
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::type_check::check;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_storages_common_cache::CachePolicy;
    use databend_storages_common_io::ReadSettings;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Statistics;
    use opendal::Operator;
    use opendal::services::Memory;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;
    use crate::segment_zone_map::format::SegmentZoneMapWriter;
    use crate::segment_zone_map::format::zone_map_columns;

    const INT_TYPE: TableDataType = TableDataType::Number(NumberDataType::Int32);

    fn schema() -> TableSchemaRef {
        Arc::new(TableSchema::new(vec![
            TableField::new("a", INT_TYPE.wrap_nullable()),
            TableField::new("b", TableDataType::String.wrap_nullable()),
        ]))
    }

    fn int(v: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(v))
    }

    fn string(v: &str) -> Scalar {
        Scalar::String(v.to_string())
    }

    /// Statistics of one column: absent, all NULL, or a random range with a
    /// random null count.
    fn random_stats(
        rng: &mut StdRng,
        value: impl Fn(&mut StdRng) -> Scalar,
    ) -> Option<ColumnStatistics> {
        match rng.gen_range(0..8) {
            0 | 1 => None,
            2 => Some(ColumnStatistics::new(
                Scalar::Null,
                Scalar::Null,
                10,
                0,
                None,
            )),
            _ => {
                let (x, y) = (value(rng), value(rng));
                let (min, max) = if x <= y { (x, y) } else { (y, x) };
                Some(ColumnStatistics::new(
                    min,
                    max,
                    rng.gen_range(0..3),
                    0,
                    None,
                ))
            }
        }
    }

    fn random_summary(rng: &mut StdRng) -> Statistics {
        let mut col_stats = StatisticsOfColumns::new();
        if let Some(stats) = random_stats(rng, |rng| int(rng.gen_range(-5..5))) {
            col_stats.insert(0, stats);
        }
        if let Some(stats) =
            random_stats(rng, |rng| string(["a", "b", "c", "d"][rng.gen_range(0..4)]))
        {
            col_stats.insert(1, stats);
        }
        Statistics {
            block_count: 1,
            col_stats,
            ..Default::default()
        }
    }

    fn column(name: &str, data_type: TableDataType) -> RawExpr<String> {
        let (id, display_name) = (name.to_string(), name.to_string());
        RawExpr::ColumnRef {
            span: None,
            id,
            data_type: (&data_type).into(),
            display_name,
        }
    }

    fn call(name: &str, args: Vec<RawExpr<String>>) -> RawExpr<String> {
        RawExpr::FunctionCall {
            span: None,
            name: name.to_string(),
            params: vec![],
            args,
        }
    }

    fn constant(scalar: Scalar) -> RawExpr<String> {
        RawExpr::Constant {
            span: None,
            scalar,
            data_type: None,
        }
    }

    fn random_predicate(rng: &mut StdRng, depth: usize) -> RawExpr<String> {
        let a = || column("a", INT_TYPE.wrap_nullable());
        let b = || column("b", TableDataType::String.wrap_nullable());
        let cmp = ["eq", "noteq", "gt", "gte", "lt", "lte"][rng.gen_range(0..6)];
        let sub = |rng: &mut StdRng| random_predicate(rng, depth - 1);
        match rng.gen_range(0..if depth == 0 { 5 } else { 8 }) {
            0 => call(cmp, vec![a(), constant(int(rng.gen_range(-6..6)))]),
            1 => call(cmp, vec![
                b(),
                constant(string(["", "a", "b", "c", "d", "e"][rng.gen_range(0..6)])),
            ]),
            // `IS NULL` is lowered to `not(is_not_null(x))`.
            2 => call("not", vec![call("is_not_null", vec![a()])]),
            3 => call("is_not_null", vec![a()]),
            4 => call("not", vec![call("is_not_null", vec![b()])]),
            5 => call("and", vec![sub(rng), sub(rng)]),
            6 => call("or", vec![sub(rng), sub(rng)]),
            _ => call("not", vec![sub(rng)]),
        }
    }

    /// Whatever the zone map drops, pruning on the segment summary itself drops as
    /// well; covered segments get exactly the same verdict, uncovered ones are kept.
    #[tokio::test]
    async fn test_zone_map_pruning_matches_segment_pruning() -> Result<()> {
        let mut rng = StdRng::seed_from_u64(0x5e91d);
        let op = Operator::new(Memory::default())?.finish();
        let schema = schema();
        let func_ctx = FunctionContext::default();

        // Three row groups: one whose segments all share one summary, so that
        // its bounds can fail a filter as a whole, and two random ones.
        let (group_rows, total) = (20, 60);
        let shared = random_summary(&mut rng);
        let summaries: Vec<Statistics> = (0..total)
            .map(|i| match i < group_rows {
                true => shared.clone(),
                false => random_summary(&mut rng),
            })
            .collect();
        let segments: Vec<(usize, Location)> =
            (0..total).map(|i| (i, (format!("seg_{i}"), 4))).collect();
        // The last ten segments are not covered.
        let covered = total - 10;
        let location = ("prune/zm.parquet".to_string(), SegmentZoneMapMeta::VERSION);
        let columns = zone_map_columns(&schema);
        let writer = SegmentZoneMapWriter::create(&op, location, columns, 1.0).await?;
        let mut writer = writer.with_row_group_rows(group_rows);
        for (i, summary) in summaries.iter().take(covered).enumerate() {
            writer.push_summary(&segments[i].1.0, summary).await?;
        }
        let meta = writer.finish().await?;

        crate::test_utils::init_test_globals()?;
        let settings = ReadSettings {
            max_gap_size: 48,
            max_range_size: 512 * 1024,
            parquet_fast_read_bytes: 0,
        };
        let reader = SegmentZoneMapReader::new(op.clone(), CachePolicy::DEFAULT, settings);
        let mut dropped_any = false;
        for round in 0..300 {
            let filter = check(&random_predicate(&mut rng, 2), &BUILTIN_FUNCTIONS)?;
            let kept = prune_segments_by_zone_map(
                &reader,
                &meta,
                func_ctx.clone(),
                &schema,
                &filter,
                &segments,
                1 + round % 3,
            );
            let kept: HashSet<usize> = kept.await?.into_iter().map(|(idx, _)| idx).collect();
            let pruner = RangePrunerCreator::try_create(func_ctx.clone(), &schema, Some(&filter))?;
            for (i, summary) in summaries.iter().enumerate() {
                let direct =
                    pruner.should_keep(&RangeIndexInput::from_columns(&summary.col_stats), None);
                let expected = direct || i >= covered;
                assert_eq!(
                    kept.contains(&i),
                    expected,
                    "segment {i} {:?} under {filter}",
                    summary.col_stats
                );
                dropped_any |= !direct;
            }
        }
        assert!(dropped_any, "the predicates never pruned anything");
        Ok(())
    }
}
