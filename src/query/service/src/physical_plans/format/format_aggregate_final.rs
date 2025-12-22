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

use crate::physical_plans::AggregateFinal;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::pretty_display_agg_desc;

pub struct AggregateFinalFormatter<'a> {
    inner: &'a AggregateFinal,
}

impl<'a> AggregateFinalFormatter<'a> {
    pub fn create(inner: &'a AggregateFinal) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(AggregateFinalFormatter { inner })
    }
}

impl<'a> PhysicalFormat for AggregateFinalFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let group_by = self
            .inner
            .group_by
            .iter()
            .map(|&index| {
                let name = ctx.metadata.column(index).name();
                Ok(name)
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");

        let agg_funcs = self
            .inner
            .agg_funcs
            .iter()
            .map(|agg| pretty_display_agg_desc(agg, ctx.metadata))
            .collect::<Vec<_>>()
            .join(", ");

        let output_schema = self.inner.output_schema()?;
        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(output_schema, ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("group by: [{group_by}]")),
            FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
        ];

        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            node_children.extend(items);
        }

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);
        Ok(FormatTreeNode::with_children(
            "AggregateFinal".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.format_join(ctx)
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.partial_format(ctx)
    }
}
