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

use std::time::Duration;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::DUMMY_TABLE_INDEX;
use itertools::Itertools;

use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::part_stats_info_to_format_tree;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::TableScan;

pub struct TableScanFormatter<'a> {
    inner: &'a TableScan,
}

impl<'a> TableScanFormatter<'a> {
    pub fn create(inner: &'a TableScan) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(TableScanFormatter { inner })
    }
}

impl<'a> PhysicalFormat for TableScanFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.table_index == Some(DUMMY_TABLE_INDEX) {
            return Ok(FormatTreeNode::new("DummyTableScan".to_string()));
        }

        let table_name = match self.inner.table_index {
            None => format!(
                "{}.{}",
                self.inner.source.source_info.catalog_name(),
                self.inner.source.source_info.desc()
            ),
            Some(table_index) => {
                let table = ctx.metadata.table(table_index).clone();
                format!("{}.{}.{}", table.catalog(), table.database(), table.name())
            }
        };
        let filters = self
            .inner
            .source
            .push_downs
            .as_ref()
            .and_then(|extras| {
                extras
                    .filters
                    .as_ref()
                    .map(|filters| filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            })
            .unwrap_or_default();

        let limit = self
            .inner
            .source
            .push_downs
            .as_ref()
            .map_or("NONE".to_string(), |extras| {
                extras
                    .limit
                    .map_or("NONE".to_string(), |limit| limit.to_string())
            });

        let virtual_columns = self.inner.source.push_downs.as_ref().and_then(|extras| {
            extras.virtual_column.as_ref().map(|virtual_column| {
                let mut names = virtual_column
                    .virtual_column_fields
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>();
                names.sort();
                names.iter().join(", ")
            })
        });

        let agg_index = self
            .inner
            .source
            .push_downs
            .as_ref()
            .and_then(|extras| extras.agg_index.as_ref());

        let mut children = vec![
            FormatTreeNode::new(format!("table: {table_name}")),
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, false)
            )),
        ];

        // Part stats.
        children.extend(part_stats_info_to_format_tree(
            &self.inner.source.statistics,
        ));
        // Push downs.
        let push_downs = format!("push downs: [filters: [{filters}], limit: {limit}]");
        children.push(FormatTreeNode::new(push_downs));

        // runtime filters
        if let Some(filters) = ctx.scan_id_to_runtime_filters.get(&self.inner.scan_id) {
            if !filters.is_empty() {
                let reports = ctx.runtime_filter_reports.get(&self.inner.scan_id);
                let mut filter_nodes = Vec::new();
                for filter in filters.iter() {
                    if let Some((probe_key, _)) = filter
                        .probe_targets
                        .iter()
                        .find(|(_, scan_id)| scan_id == &self.inner.scan_id)
                    {
                        let build_expr = filter.build_key.as_expr(&BUILTIN_FUNCTIONS).sql_display();
                        let probe_expr = probe_key.as_expr(&BUILTIN_FUNCTIONS).sql_display();

                        let mut types = Vec::new();
                        if filter.enable_bloom_runtime_filter {
                            types.push("bloom");
                        }
                        if filter.enable_inlist_runtime_filter {
                            types.push("inlist");
                        }
                        if filter.enable_min_max_runtime_filter {
                            types.push("min_max");
                        }
                        let type_text = if types.is_empty() {
                            String::from("none")
                        } else {
                            types.join(",")
                        };

                        let mut detail_children =
                            vec![FormatTreeNode::new(format!("type: [{}]", type_text))];

                        if let Some(reports) = reports {
                            if let Some(report) =
                                reports.iter().find(|report| report.filter_id == filter.id)
                            {
                                if report.has_bloom {
                                    detail_children.push(FormatTreeNode::new(format!(
                                        "bloom rows filtered: {}",
                                        report.stats.bloom_rows_filtered
                                    )));
                                    detail_children.push(FormatTreeNode::new(format!(
                                        "bloom time: {:?}",
                                        Duration::from_nanos(report.stats.bloom_time_ns)
                                    )));
                                }
                                if report.has_inlist || report.has_min_max {
                                    detail_children.push(FormatTreeNode::new(format!(
                                        "inlist/min-max time: {:?}",
                                        Duration::from_nanos(report.stats.inlist_min_max_time_ns)
                                    )));
                                    detail_children.push(FormatTreeNode::new(format!(
                                        "min-max rows filtered: {}",
                                        report.stats.min_max_rows_filtered
                                    )));
                                    detail_children.push(FormatTreeNode::new(format!(
                                        "min-max partitions pruned: {}",
                                        report.stats.min_max_partitions_pruned
                                    )));
                                }
                            }
                        }

                        let header = format!(
                            "filter id:{}, build:{}, probe:{}",
                            filter.id, build_expr, probe_expr
                        );
                        filter_nodes.push(FormatTreeNode::with_children(header, detail_children));
                    }
                }

                if !filter_nodes.is_empty() {
                    children.push(FormatTreeNode::with_children(
                        "runtime filters:".to_string(),
                        filter_nodes,
                    ));
                }
            }
        }

        // Virtual columns.
        if let Some(virtual_columns) = virtual_columns {
            if !virtual_columns.is_empty() {
                let virtual_columns = format!("virtual columns: [{virtual_columns}]");
                children.push(FormatTreeNode::new(virtual_columns));
            }
        }

        // Aggregating index
        if let Some(agg_index) = agg_index {
            let (_, agg_index_sql, _) = ctx
                .metadata
                .get_agg_indexes(&table_name)
                .unwrap()
                .iter()
                .find(|(index, _, _)| *index == agg_index.index_id)
                .unwrap();

            children.push(FormatTreeNode::new(format!(
                "aggregating index: [{agg_index_sql}]"
            )));

            let agg_sel = agg_index
                .selection
                .iter()
                .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", ");
            let agg_filter = agg_index
                .filter
                .as_ref()
                .map(|f| f.as_expr(&BUILTIN_FUNCTIONS).sql_display());
            let text = if let Some(f) = agg_filter {
                format!("rewritten query: [selection: [{agg_sel}], filter: {f}]")
            } else {
                format!("rewritten query: [selection: [{agg_sel}]]")
            };
            children.push(FormatTreeNode::new(text));
        }

        if let Some(info) = &self.inner.stat_info {
            children.extend(plan_stats_info_to_format_tree(info));
        }

        Ok(FormatTreeNode::with_children(
            "TableScan".to_string(),
            children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.table_index == Some(DUMMY_TABLE_INDEX) {
            return Ok(FormatTreeNode::with_children(
                format!(
                    "Scan: dummy, rows: {}",
                    self.inner.source.statistics.read_rows
                ),
                vec![],
            ));
        }

        match self.inner.table_index {
            None => Ok(FormatTreeNode::with_children(
                format!(
                    "Scan: {}.{} (read rows: {})",
                    self.inner.source.source_info.catalog_name(),
                    self.inner.source.source_info.desc(),
                    self.inner.source.statistics.read_rows
                ),
                vec![],
            )),
            Some(table_index) => {
                let table = ctx.metadata.table(table_index).clone();
                let table_name =
                    format!("{}.{}.{}", table.catalog(), table.database(), table.name());

                Ok(FormatTreeNode::with_children(
                    format!(
                        "Scan: {} (#{}) (read rows: {})",
                        table_name, table_index, self.inner.source.statistics.read_rows
                    ),
                    vec![],
                ))
            }
        }
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.table_index == Some(DUMMY_TABLE_INDEX) {
            return Ok(FormatTreeNode::new("DummyTableScan".to_string()));
        }
        let table_name = match self.inner.table_index {
            None => format!(
                "{}.{}",
                self.inner.source.source_info.catalog_name(),
                self.inner.source.source_info.desc()
            ),
            Some(table_index) => {
                let table = ctx.metadata.table(table_index).clone();
                format!("{}.{}.{}", table.catalog(), table.database(), table.name())
            }
        };
        let mut children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            children.extend(items);
        }

        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());

        Ok(FormatTreeNode::with_children(
            "TableScan".to_string(),
            children,
        ))
    }
}
