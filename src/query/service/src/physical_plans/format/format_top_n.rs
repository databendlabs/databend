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
use databend_common_sql::executor::physical_plans::SortDesc;

use crate::physical_plans::FinalTopNPlan;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PartialTopNPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;

pub struct PartialTopNFormatter<'a> {
    inner: &'a PartialTopNPlan,
}

impl<'a> PartialTopNFormatter<'a> {
    pub fn create(inner: &'a PartialTopNPlan) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(Self { inner })
    }
}

impl PhysicalFormat for PartialTopNFormatter<'_> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = top_n_details(
            self.inner.output_schema()?,
            &self.inner.order_by,
            self.inner.candidate_count,
            0,
            ctx,
        );
        if let Some(info) = &self.inner.stat_info {
            children.extend(plan_stats_info_to_format_tree(info));
        }
        children.push(self.inner.input.formatter()?.dispatch(ctx)?);
        Ok(FormatTreeNode::with_children(
            "TopN(Partial)".to_string(),
            children,
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

pub struct FinalTopNFormatter<'a> {
    inner: &'a FinalTopNPlan,
}

impl<'a> FinalTopNFormatter<'a> {
    pub fn create(inner: &'a FinalTopNPlan) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(Self { inner })
    }
}

impl PhysicalFormat for FinalTopNFormatter<'_> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = top_n_details(
            self.inner.output_schema()?,
            &self.inner.order_by,
            self.inner.limit,
            self.inner.offset,
            ctx,
        );
        if let Some(info) = &self.inner.stat_info {
            children.extend(plan_stats_info_to_format_tree(info));
        }
        children.push(self.inner.input.formatter()?.dispatch(ctx)?);
        Ok(FormatTreeNode::with_children(
            "TopN(Final)".to_string(),
            children,
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

fn top_n_details(
    schema: databend_common_expression::DataSchemaRef,
    order_by: &[SortDesc],
    limit: usize,
    offset: usize,
    ctx: &FormatContext<'_>,
) -> Vec<FormatTreeNode<String>> {
    let sort_keys = order_by
        .iter()
        .map(|sort_key| {
            format!(
                "{} {} {}",
                sort_key.display_name,
                if sort_key.asc { "ASC" } else { "DESC" },
                if sort_key.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(schema, ctx.metadata, true)
        )),
        FormatTreeNode::new(format!("sort keys: [{sort_keys}]")),
        FormatTreeNode::new(format!("limit: {limit}")),
        FormatTreeNode::new(format!("offset: {offset}")),
    ]
}
