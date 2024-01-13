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
use std::sync::Arc;

use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_pruner::RangePrunerCreator;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::metadata::ParquetMetaData;
use parquet::format::PageLocation;

use super::statistics::collect_row_group_stats;
use crate::parquet_rs::statistics::convert_index_to_column_statistics;

/// A pruner to prune row groups and pages of a parquet files.
///
/// We can use this pruner to compute row groups and pages to skip.
#[derive(Clone)]
pub struct ParquetRSPruner {
    leaf_fields: Arc<Vec<TableField>>,
    range_pruner: Option<(
        Arc<dyn RangePruner + Send + Sync>,
        Arc<dyn RangePruner + Send + Sync>,
    )>,
    prune_row_groups: bool,
    prune_pages: bool,

    /// Leaf ids of columns contained in filter predicates.
    predicate_columns: Vec<usize>,
}

impl ParquetRSPruner {
    pub fn try_create(
        func_ctx: FunctionContext,
        schema: TableSchemaRef,
        leaf_fields: Arc<Vec<TableField>>,
        push_down: &Option<PushDownInfo>,
        options: ParquetReadOptions,
        partition_columns: Vec<String>,
    ) -> Result<Self> {
        // Build `RangePruner` by `filter`.
        let filter = push_down.as_ref().and_then(|p| p.filters.as_ref());

        let mut predicate_columns = vec![];
        let range_pruner =
            if filter.is_some() && (options.prune_row_groups() || options.prune_pages()) {
                let filter_expr = filter.as_ref().unwrap().filter.as_expr(&BUILTIN_FUNCTIONS);
                let inverted_filter_expr = filter
                    .as_ref()
                    .unwrap()
                    .inverted_filter
                    .as_expr(&BUILTIN_FUNCTIONS);

                predicate_columns = filter_expr
                    .column_refs()
                    .into_keys()
                    .filter_map(|name| {
                        leaf_fields
                            .iter()
                            .position(|f| f.name.eq_ignore_ascii_case(&name))
                            .or_else(|| {
                                // check partition columns
                                partition_columns
                                    .iter()
                                    .position(|c| c.eq_ignore_ascii_case(&name))
                                    .unwrap();
                                None
                            })
                    })
                    .collect::<Vec<_>>();
                predicate_columns.sort();
                let pruner =
                    RangePrunerCreator::try_create(func_ctx.clone(), &schema, Some(&filter_expr))?;
                let inverted_pruner =
                    RangePrunerCreator::try_create(func_ctx, &schema, Some(&inverted_filter_expr))?;
                Some((pruner, inverted_pruner))
            } else {
                None
            };

        Ok(ParquetRSPruner {
            leaf_fields,
            range_pruner,
            prune_row_groups: options.prune_row_groups(),
            prune_pages: options.prune_pages(),
            predicate_columns,
        })
    }

    /// Prune row groups of a parquet file.
    ///
    /// Return the selected row groups' indices in the meta and omit filter flags.
    ///
    /// If `stats` is not [None], we use this statistics to prune but not collect again.
    ///
    /// `partition_values` is used only for Delta table engine.
    pub fn prune_row_groups(
        &self,
        meta: &ParquetMetaData,
        stats: Option<&[StatisticsOfColumns]>,
        partition_values: Option<&HashMap<String, Scalar>>,
    ) -> Result<(Vec<usize>, Vec<bool>)> {
        let default_selection = (0..meta.num_row_groups()).collect();
        let default_omits = vec![false; meta.num_row_groups()];
        if !self.prune_row_groups {
            return Ok((default_selection, default_omits));
        }

        match &self.range_pruner {
            None => Ok((default_selection, default_omits)),

            Some((pruner, inverted_pruner)) => {
                let mut selection = Vec::with_capacity(meta.num_row_groups());
                let mut omits = Vec::with_capacity(meta.num_row_groups());
                if let Some(row_group_stats) = stats {
                    for (i, row_group) in row_group_stats.iter().enumerate() {
                        if pruner.should_keep_with_partition_columns(row_group, partition_values) {
                            selection.push(i);

                            let omit = !inverted_pruner
                                .should_keep_with_partition_columns(row_group, partition_values);
                            omits.push(omit);
                        }
                    }
                    Ok((selection, omits))
                } else if let Some(row_group_stats) = collect_row_group_stats(
                    meta.row_groups(),
                    &self.leaf_fields,
                    Some(&self.predicate_columns),
                ) {
                    for (i, row_group) in row_group_stats.iter().enumerate() {
                        if pruner.should_keep_with_partition_columns(row_group, partition_values) {
                            selection.push(i);

                            let omit = !inverted_pruner
                                .should_keep_with_partition_columns(row_group, partition_values);
                            omits.push(omit);
                        }
                    }
                    Ok((selection, omits))
                } else {
                    Ok((default_selection, default_omits))
                }
            }
        }
    }

    /// Prune pages of a parquet file.
    ///
    /// Return a vector of [`RowSelection`] to represent rows to read.
    /// `partition_values` is used only for Delta table engine.
    pub fn prune_pages(
        &self,
        meta: &ParquetMetaData,
        row_groups: &[usize],
        partition_values: Option<&HashMap<String, Scalar>>,
    ) -> Result<Option<RowSelection>> {
        if !self.prune_pages {
            return Ok(None);
        }
        match &self.range_pruner {
            None => Ok(None),
            Some((pruner, _)) => {
                // Only if the file has page level statistics, we can use them to prune.
                if meta.column_index().is_none() || meta.offset_index().is_none() {
                    return Ok(None);
                }
                let fields = self.leaf_fields.as_ref();
                let all_column_index = meta.column_index().unwrap();
                let all_offset_index = meta.offset_index().unwrap();
                assert_eq!(all_column_index.len(), all_offset_index.len());
                assert_eq!(all_column_index.len(), meta.num_row_groups());
                let mut selectors = Vec::with_capacity(row_groups.len());
                for r in row_groups.iter() {
                    let rg = meta.row_group(*r);
                    let column_index = &all_column_index[*r];
                    let offset_index = &all_offset_index[*r];
                    assert_eq!(column_index.len(), offset_index.len());
                    // `sel_of_different_cols` is selectors generated by different columns, merge them at last.
                    let mut sel_of_different_cols = Vec::with_capacity(column_index.len());
                    for col_idx in self.predicate_columns.iter() {
                        let index = &column_index[*col_idx];
                        let page_locations = &offset_index[*col_idx];
                        let pages_num_rows = compute_pages_num_rows(page_locations, rg.num_rows());
                        let stats = convert_index_to_column_statistics(
                            index,
                            page_locations.len(),
                            &fields[*col_idx].data_type.remove_nullable(),
                        );

                        let mut sel_of_cur_col = Vec::with_capacity(stats.len());
                        for (page_idx, stat) in stats.into_iter().enumerate() {
                            let page_num_rows = pages_num_rows[page_idx];
                            match stat {
                                Some(s) => {
                                    if !pruner.should_keep_with_partition_columns(
                                        &HashMap::from([(*col_idx as u32, s)]),
                                        partition_values,
                                    ) {
                                        sel_of_cur_col.push(RowSelector::skip(page_num_rows));
                                    } else {
                                        sel_of_cur_col.push(RowSelector::select(page_num_rows));
                                    }
                                }
                                _ => {
                                    sel_of_cur_col.push(RowSelector::select(page_num_rows));
                                }
                            }
                        }
                        sel_of_different_cols.push(sel_of_cur_col);
                    }
                    let sel_of_cur_rg = merge_row_selectors(sel_of_different_cols);
                    selectors.extend(sel_of_cur_rg);
                }
                // Trim selectors.
                while let Some(s) = selectors.last() && s.row_count == 0 {
                    selectors.pop();
                }
                Ok(Some(RowSelection::from(selectors)))
            }
        }
    }
}

fn compute_pages_num_rows(location: &[PageLocation], num_rows: i64) -> Vec<usize> {
    let mut counts = Vec::with_capacity(location.len());
    location.windows(2).for_each(|x| {
        let start = x[0].first_row_index as usize;
        let end = x[1].first_row_index as usize;
        counts.push(end - start);
    });
    counts.push((num_rows - location.last().unwrap().first_row_index) as usize);
    counts
}

fn merge_row_selectors(row_selections: Vec<Vec<RowSelector>>) -> Vec<RowSelector> {
    row_selections
        .into_iter()
        .map(RowSelection::from)
        .reduce(|s1, s2| s1.intersection(&s2))
        .unwrap()
        .into()
}
