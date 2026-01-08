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
use itertools::Itertools;

use crate::physical_plans::AggregatePartial;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::pretty_display_agg_desc;

pub struct AggregatePartialFormatter<'a> {
    inner: &'a AggregatePartial,
}

impl<'a> AggregatePartialFormatter<'a> {
    pub fn create(inner: &'a AggregatePartial) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(AggregatePartialFormatter { inner })
    }
}

impl<'a> PhysicalFormat for AggregatePartialFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[stacksafe::stacksafe]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let group_by = self
            .inner
            .group_by
            .iter()
            .map(|&index| ctx.metadata.column(index).name())
            .join(", ");

        let agg_funcs = self
            .inner
            .agg_funcs
            .iter()
            .map(|agg| pretty_display_agg_desc(agg, ctx.metadata))
            .collect::<Vec<_>>()
            .join(", ");

        let mut children = vec![
            FormatTreeNode::new(format!("group by: [{group_by}]")),
            FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
        ];

        if let Some(info) = &self.inner.stat_info {
            children.extend(plan_stats_info_to_format_tree(info));
        }

        if let Some((_, r)) = &self.inner.rank_limit {
            children.push(FormatTreeNode::new(format!("rank limit: {r}")));
        }

        let input_formatter = self.inner.input.formatter()?;
        children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "AggregatePartial".to_string(),
            children,
        ))
    }

    #[stacksafe::stacksafe]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.format_join(ctx)
    }

    #[stacksafe::stacksafe]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.partial_format(ctx)
    }
}
