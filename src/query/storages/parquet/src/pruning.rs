// Copyright 2023 Datafuse Labs.
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
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::arrow::io::parquet::read::get_field_pages;
use common_arrow::arrow::io::parquet::read::indexes::compute_page_row_intervals;
use common_arrow::arrow::io::parquet::read::indexes::read_columns_indexes;
use common_arrow::arrow::io::parquet::read::indexes::FieldPageStatistics;
use common_arrow::parquet::indexes::Interval;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::read_pages_locations;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Expr;
use common_expression::TableSchemaRef;
use common_storage::ColumnLeaves;
use storages_common_pruner::RangePruner;
use storages_common_pruner::RangePrunerCreator;

use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetRowGroupPart;
use crate::read_options::ReadOptions;
use crate::statistics::collect_row_group_stats;
use crate::statistics::BatchStatistics;

/// Try to prune parquet files and gernerate the final row group partitions.
///
/// `ctx`: the table context.
///
/// `locations`: the parquet file locations.
///
/// `schema`: the projected table schema.
///
/// `filters`: the pushed-down filters.
///
/// `columns_to_read`: the projected column indices.
///
/// `column_leaves`: the projected column leaves.
///
/// `skip_pruning`: whether to skip pruning.
///
/// `read_options`: more information can be found in [`ReadOptions`].
#[allow(clippy::too_many_arguments)]
pub fn prune_and_set_partitions(
    ctx: &Arc<dyn TableContext>,
    locations: &[String],
    schema: &TableSchemaRef,
    filters: &Option<&[Expr<String>]>,
    columns_to_read: &HashSet<usize>,
    column_leaves: &ColumnLeaves,
    skip_pruning: bool,
    read_options: &ReadOptions,
) -> Result<()> {
    let mut partitions = Vec::with_capacity(locations.len());

    let row_group_pruner = if read_options.prune_row_groups {
        Some(RangePrunerCreator::try_create(ctx, *filters, schema)?)
    } else {
        None
    };

    let page_pruners = if read_options.prune_pages && filters.is_some() {
        let filters = filters.unwrap();
        Some(build_column_page_pruners(ctx, schema, filters)?)
    } else {
        None
    };

    for location in locations {
        let mut file = File::open(location).map_err(|e| {
            ErrorCode::Internal(format!("Failed to open file '{}': {}", location, e))
        })?;
        let file_meta = pread::read_metadata(&mut file).map_err(|e| {
            ErrorCode::Internal(format!(
                "Read parquet file '{}''s meta error: {}",
                location, e
            ))
        })?;
        let mut row_group_pruned = vec![false; file_meta.row_groups.len()];

        let no_stats = file_meta.row_groups.iter().any(|r| {
            r.columns()
                .iter()
                .any(|c| c.metadata().statistics.is_none())
        });

        if read_options.prune_row_groups && !skip_pruning && !no_stats {
            let pruner = row_group_pruner.as_ref().unwrap();
            // If collecting stats fails or `should_keep` is true, we still read the row group.
            // Otherwise, the row group will be pruned.
            if let Ok(row_group_stats) =
                collect_row_group_stats(column_leaves, &file_meta.row_groups)
            {
                for (idx, (stats, _rg)) in row_group_stats
                    .iter()
                    .zip(file_meta.row_groups.iter())
                    .enumerate()
                {
                    row_group_pruned[idx] = !pruner.should_keep(stats);
                }
            }
        }

        for (idx, rg) in file_meta.row_groups.iter().enumerate() {
            if row_group_pruned[idx] {
                continue;
            }

            let row_selection = if read_options.prune_pages {
                page_pruners
                    .as_ref()
                    .map(|pruners| filter_pages(&mut file, schema, rg, pruners))
                    .transpose()?
            } else {
                None
            };

            let mut column_metas = HashMap::with_capacity(columns_to_read.len());
            for index in columns_to_read {
                let c = &rg.columns()[*index];
                let (offset, length) = c.byte_range();
                column_metas.insert(*index, ColumnMeta {
                    offset,
                    length,
                    compression: c.compression(),
                });
            }

            partitions.push(ParquetRowGroupPart::create(
                location.clone(),
                rg.num_rows(),
                column_metas,
                row_selection,
            ))
        }
    }
    ctx.try_set_partitions(Partitions::create(PartitionsShuffleKind::Mod, partitions))?;
    Ok(())
}

/// [`RangePruner`]s for each column
type ColumnRangePruners = Vec<(usize, Arc<dyn RangePruner + Send + Sync>)>;

/// Build page pruner of each column.
/// Only one column expression can be used to build the page pruner.
fn build_column_page_pruners(
    ctx: &Arc<dyn TableContext>,
    schema: &TableSchemaRef,
    filters: &[Expr<String>],
) -> Result<ColumnRangePruners> {
    let mut pruner_per_col: HashMap<String, Vec<Expr<String>>> = HashMap::new();
    for expr in filters {
        let columns = expr.column_refs();
        if columns.len() != 1 {
            continue;
        }
        let (col_name, _) = columns.iter().next().unwrap();
        pruner_per_col
            .entry(col_name.to_string())
            .and_modify(|f| f.push(expr.clone()))
            .or_insert_with(|| vec![expr.clone()]);
    }
    pruner_per_col
        .iter()
        .map(|(k, v)| {
            let filter = RangePrunerCreator::try_create(ctx, Some(v), schema)?;
            let col_idx = schema.index_of(k)?;
            Ok((col_idx, filter))
        })
        .collect()
}

/// Filter pages by filter expression.
///
/// Returns the final selection of rows.
fn filter_pages<R: Read + Seek>(
    reader: &mut R,
    schema: &TableSchemaRef,
    row_group: &RowGroupMetaData,
    pruners: &ColumnRangePruners,
) -> Result<Vec<Interval>> {
    let mut fields = Vec::with_capacity(pruners.len());
    for (col_idx, _) in pruners {
        let field: ArrowField = schema.field(*col_idx).into();
        fields.push(field);
    }

    let num_rows = row_group.num_rows();

    // one vec per column
    let locations = read_pages_locations(reader, row_group.columns())?;
    // one Vec<Vec<>> per field (non-nested contain a single entry on the first column)
    let locations = fields
        .iter()
        .map(|field| get_field_pages(row_group.columns(), &locations, &field.name))
        .collect::<Vec<_>>();

    // one ColumnPageStatistics per field
    let page_stats = read_columns_indexes(reader, row_group.columns(), &fields)?;

    let intervals = locations
        .iter()
        .map(|locations| {
            locations
                .iter()
                .map(|locations| Ok(compute_page_row_intervals(locations, num_rows)?))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    // Currently, only non-nested types are supported.
    let mut row_selections = Vec::with_capacity(pruners.len());
    for (i, (col_offset, pruner)) in pruners.iter().enumerate() {
        let stat = &page_stats[i];
        let page_intervals = &intervals[i][0];
        let data_type = schema.field(*col_offset).data_type();

        let mut row_selection = vec![];
        match stat {
            FieldPageStatistics::Single(stats) => {
                let stats = BatchStatistics::from_column_statistics(stats, &data_type.into())?;
                for (page_num, intv) in page_intervals.iter().enumerate() {
                    let stat = stats.get(page_num);
                    if pruner.should_keep(&HashMap::from([(*col_offset as u32, stat)])) {
                        row_selection.push(*intv);
                    }
                }
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Only non-nested types are supported in page filter.",
                ));
            }
        }
        row_selections.push(row_selection);
    }

    Ok(combine_intervals(row_selections))
}

/// Combine row selection of each column into a final selection of the whole row group.
fn combine_intervals(row_selections: Vec<Vec<Interval>>) -> Vec<Interval> {
    if row_selections.is_empty() {
        return vec![];
    }
    let mut res = row_selections[0].clone();
    for sel in row_selections.iter().skip(1) {
        res = and_intervals(&res, sel);
    }
    res
}

/// Do "and" operation on two row selections.
/// Select the rows which both `sel1` and `sel2` select.
fn and_intervals(sel1: &[Interval], sel2: &[Interval]) -> Vec<Interval> {
    let mut res = vec![];

    for sel in sel1 {
        res.extend(is_in(*sel, sel2));
    }
    res
}

/// If `probe` overlaps with `intervals`,
/// return the overlapping part of `probe` in `intervals`.
/// Otherwise, return an empty vector.
fn is_in(probe: Interval, intervals: &[Interval]) -> Vec<Interval> {
    intervals
        .iter()
        .filter_map(|interval| {
            let interval_end = interval.start + interval.length;
            let probe_end = probe.start + probe.length;
            let overlaps = (probe.start < interval_end) && (probe_end > interval.start);
            if overlaps {
                let start = interval.start.max(probe.start);
                let end = interval_end.min(probe_end);
                Some(Interval::new(start, end - start))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use common_arrow::parquet::indexes::Interval;

    use crate::pruning::and_intervals;
    use crate::pruning::combine_intervals;

    #[test]
    fn test_and_intervals() {
        // [12, 35), [38, 43)
        let sel1 = vec![Interval::new(12, 23), Interval::new(38, 5)];
        // [0, 5), [9, 24), [30, 40)
        let sel2 = vec![
            Interval::new(0, 5),
            Interval::new(9, 15),
            Interval::new(30, 10),
        ];

        // [12, 24), [30, 35), [38, 40)
        let expected = vec![
            Interval::new(12, 12),
            Interval::new(30, 5),
            Interval::new(38, 2),
        ];
        let actual = and_intervals(&sel1, &sel2);

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_combine_intervals() {
        // sel1: [12, 35), [38, 43)
        // sel2: [0, 5), [9, 24), [30, 40)
        // sel3: [1,2), [4, 31), [30, 41)
        let intervals = vec![
            vec![Interval::new(12, 23), Interval::new(38, 5)],
            vec![
                Interval::new(0, 5),
                Interval::new(9, 15),
                Interval::new(30, 10),
            ],
            vec![
                Interval::new(1, 1),
                Interval::new(4, 27),
                Interval::new(39, 2),
            ],
        ];

        // [12, 24), [30, 31), [39, 40)
        let expected = vec![
            Interval::new(12, 12),
            Interval::new(30, 1),
            Interval::new(39, 1),
        ];

        let actual = combine_intervals(intervals);

        assert_eq!(expected, actual);
    }
}
