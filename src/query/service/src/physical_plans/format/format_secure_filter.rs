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

use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::SecureFilter;

pub struct SecureFilterFormatter<'a> {
    inner: &'a SecureFilter,
}

impl<'a> SecureFilterFormatter<'a> {
    pub fn create(inner: &'a SecureFilter) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(SecureFilterFormatter { inner })
    }
}

impl<'a> PhysicalFormat for SecureFilterFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let filter = self
            .inner
            .predicates
            .iter()
            .map(|pred| format!("SECURE: {}", pred.as_expr(&BUILTIN_FUNCTIONS).sql_display()))
            .join(", ");

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("secure filters: [{filter}]")),
        ];

        if let Some(info) = &self.inner.stat_info {
            node_children.extend(plan_stats_info_to_format_tree(info));
        }

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "SecureFilter".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.format_join(ctx)
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let filter = self
            .inner
            .predicates
            .iter()
            .map(|pred| format!("SECURE: {}", pred.as_expr(&BUILTIN_FUNCTIONS).sql_display()))
            .join(", ");
        let mut children = vec![FormatTreeNode::new(format!("secure filters: [{filter}]"))];
        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            children.extend(items);
        }

        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());
        let input_formatter = self.inner.input.formatter()?;
        children.push(input_formatter.partial_format(ctx)?);

        Ok(FormatTreeNode::with_children(
            "SecureFilter".to_string(),
            children,
        ))
    }
}
