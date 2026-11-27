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

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalCteRuntimeFilter;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::Sequence;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::display_materialized_cte_name;

pub struct SequenceFormatter<'a> {
    inner: &'a Sequence,
}

impl<'a> SequenceFormatter<'a> {
    pub fn create(inner: &'a Sequence) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(SequenceFormatter { inner })
    }
}

impl<'a> PhysicalFormat for SequenceFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::with_capacity(3);
        if !self.inner.cte_runtime_filters.is_empty() {
            // The producer scans are formatted below and list these filters.
            for cte_filter in &self.inner.cte_runtime_filters {
                for (_, scan_id) in &cte_filter.filter.probe_targets {
                    ctx.scan_id_to_runtime_filters
                        .entry(*scan_id)
                        .or_default()
                        .push(cte_filter.filter.clone());
                }
            }
            let filters = self
                .inner
                .cte_runtime_filters
                .iter()
                .map(|filter| FormatTreeNode::new(format_cte_runtime_filter(filter)))
                .collect();
            children.push(FormatTreeNode::with_children(
                "cte join filters:".to_string(),
                filters,
            ));
        }
        children.push(self.inner.left.formatter()?.dispatch(ctx)?);
        children.push(self.inner.right.formatter()?.dispatch(ctx)?);
        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let children = vec![
            self.inner.left.formatter()?.format_join(ctx)?,
            self.inner.right.formatter()?.format_join(ctx)?,
        ];
        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let children = vec![
            self.inner.left.formatter()?.partial_format(ctx)?,
            self.inner.right.formatter()?.partial_format(ctx)?,
        ];
        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            children,
        ))
    }
}

fn format_cte_runtime_filter(cte_filter: &PhysicalCteRuntimeFilter) -> String {
    let filter = &cte_filter.filter;
    let probe_targets = filter
        .probe_targets
        .iter()
        .map(|(probe_key, scan_id)| {
            (
                *scan_id,
                probe_key.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
            )
        })
        .sorted()
        .map(|(scan_id, probe_key)| format!("{probe_key}@scan{scan_id}"))
        .join(", ");
    let sources = cte_filter
        .source_filters
        .iter()
        .map(|id| format!("#{id}"))
        .join(", ");
    let filter_types = [
        (filter.enable_bloom_runtime_filter, "bloom"),
        (filter.enable_inlist_runtime_filter, "inlist"),
        (filter.enable_min_max_runtime_filter, "min_max"),
    ]
    .into_iter()
    .filter_map(|(enabled, name)| enabled.then_some(name))
    .join(",");
    format!(
        "filter id:{}, cte:{}, union of:[{}], probe targets:[{}], filter type:{}",
        filter.id,
        display_materialized_cte_name(&cte_filter.cte_name),
        sources,
        probe_targets,
        filter_types,
    )
}
