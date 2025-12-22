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

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::UnionAll;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;

pub struct UnionAllFormatter<'a> {
    inner: &'a UnionAll,
}

impl<'a> UnionAllFormatter<'a> {
    pub fn create(inner: &'a UnionAll) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(UnionAllFormatter { inner })
    }
}

impl<'a> PhysicalFormat for UnionAllFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut node_children = vec![FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
        ))];

        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            node_children.extend(items);
        }

        let root = if !self.inner.cte_scan_names.is_empty() {
            "UnionAll(recursive cte)".to_string()
        } else {
            "UnionAll".to_string()
        };

        let left_formatter = self.inner.left.formatter()?;
        node_children.push(left_formatter.dispatch(ctx)?);

        let right_formatter = self.inner.right.formatter()?;
        node_children.push(right_formatter.dispatch(ctx)?);
        Ok(FormatTreeNode::with_children(root, node_children))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let left_child = self.inner.left.formatter()?.format_join(ctx)?;
        let right_child = self.inner.right.formatter()?.format_join(ctx)?;

        let children = vec![
            FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
            FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
        ];

        Ok(FormatTreeNode::with_children(
            "UnionAll".to_string(),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let left_child = self.inner.left.formatter()?.partial_format(ctx)?;
        let right_child = self.inner.right.formatter()?.partial_format(ctx)?;

        let mut children = vec![];
        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            children.extend(items);
        }

        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());

        let children = vec![
            FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
            FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
        ];

        Ok(FormatTreeNode::with_children(
            "UnionAll".to_string(),
            children,
        ))
    }
}
