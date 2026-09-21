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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use crate::physical_plans::HashJoin;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;

pub struct HashJoinFormatter<'a> {
    inner: &'a HashJoin,
}

impl<'a> HashJoinFormatter<'a> {
    pub fn create(inner: &'a HashJoin) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(HashJoinFormatter { inner })
    }
}

impl<'a> PhysicalFormat for HashJoinFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        // Register runtime filters for all probe targets
        for rf in self.inner.runtime_filter.filters.iter() {
            for (_probe_key, scan_id) in &rf.probe_targets {
                ctx.scan_id_to_runtime_filters
                    .entry(*scan_id)
                    .or_default()
                    .push(rf.clone());
            }
        }

        let build_keys = self
            .inner
            .build_keys
            .iter()
            .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(", ");

        let probe_keys = self
            .inner
            .probe_keys
            .iter()
            .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(", ");

        let is_null_equal = self
            .inner
            .is_null_equal
            .iter()
            .map(|b| format!("{b}"))
            .join(", ");

        let filters = self
            .inner
            .non_equi_conditions
            .iter()
            .map(|filter| filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(", ");

        let mut build_runtime_filters = vec![];
        for rf in self.inner.runtime_filter.filters.iter() {
            // Format all probe targets (sorted for stable explain output)
            let mut probe_targets = rf
                .probe_targets
                .iter()
                .map(|(probe_key, scan_id)| {
                    (
                        probe_key.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                        *scan_id,
                    )
                })
                .collect::<Vec<_>>();

            // Sort by scan_id first, then by probe key string
            probe_targets.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

            let probe_targets_str = probe_targets
                .into_iter()
                .map(|(probe_key_str, scan_id)| format!("{}@scan{}", probe_key_str, scan_id))
                .collect::<Vec<_>>()
                .join(", ");

            let mut s = format!(
                "filter id:{}, build key:{}, probe targets:[{}], filter type:",
                rf.id,
                rf.build_key.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                probe_targets_str,
            );
            if rf.enable_bloom_runtime_filter {
                s += "bloom,";
            }
            if rf.enable_inlist_runtime_filter {
                s += "inlist,";
            }
            if rf.enable_min_max_runtime_filter {
                s += "min_max,";
            }
            s = s.trim_end_matches(',').to_string();
            build_runtime_filters.push(FormatTreeNode::new(s));
        }

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("join type: {}", self.inner.join_type)),
            FormatTreeNode::new(format!("build keys: [{build_keys}]")),
            FormatTreeNode::new(format!("probe keys: [{probe_keys}]")),
            FormatTreeNode::new(format!("keys is null equal: [{is_null_equal}]")),
            FormatTreeNode::new(format!("filters: [{filters}]")),
        ];

        if !build_runtime_filters.is_empty() {
            if self.inner.broadcast_id.is_some() {
                node_children.push(FormatTreeNode::with_children(
                    format!("build join filters(distributed):"),
                    build_runtime_filters,
                ));
            } else {
                node_children.push(FormatTreeNode::with_children(
                    format!("build join filters:"),
                    build_runtime_filters,
                ));
            }
        }

        if let Some((cache_index, column_map)) = &self.inner.build_side_cache_info {
            let mut column_indexes = column_map.keys().collect::<Vec<_>>();
            column_indexes.sort();
            node_children.push(FormatTreeNode::new(format!("cache index: {}", cache_index)));
            node_children.push(FormatTreeNode::new(format!(
                "cache columns: {:?}",
                column_indexes
            )));
        }

        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            node_children.extend(items);
        }

        let build_formatter = self.inner.build.formatter()?;
        let mut build_payload = build_formatter.dispatch(ctx)?;
        build_payload.payload = format!("{}(Build)", build_payload.payload);

        let probe_formatter = self.inner.probe.formatter()?;
        let mut probe_payload = probe_formatter.dispatch(ctx)?;
        probe_payload.payload = format!("{}(Probe)", probe_payload.payload);

        node_children.push(build_payload);
        node_children.push(probe_payload);

        Ok(FormatTreeNode::with_children(
            "HashJoin".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let build_formatter = self.inner.build.formatter()?;
        let build_child = build_formatter.format_join(ctx)?;

        let probe_formatter = self.inner.probe.formatter()?;
        let probe_child = probe_formatter.format_join(ctx)?;

        let children = vec![
            FormatTreeNode::with_children("Build".to_string(), vec![build_child]),
            FormatTreeNode::with_children("Probe".to_string(), vec![probe_child]),
        ];

        let _estimated_rows = if let Some(info) = &self.inner.stat_info {
            format!("{0:.2}", info.estimated_rows)
        } else {
            String::from("None")
        };

        Ok(FormatTreeNode::with_children(
            format!("HashJoin: {}", self.inner.join_type),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let build_child = self.inner.build.formatter()?.partial_format(ctx)?;
        let probe_child = self.inner.probe.formatter()?.partial_format(ctx)?;

        let mut children = vec![];
        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            children.extend(items);
        }

        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());

        children.push(build_child);
        children.push(probe_child);

        Ok(FormatTreeNode::with_children(
            format!("HashJoin: {}", self.inner.join_type),
            children,
        ))
    }
}
