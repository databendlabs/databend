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
use databend_common_sql::binder::MutationType;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MutationSource;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::part_stats_info_to_format_tree;

pub struct MutationSourceFormatter<'a> {
    inner: &'a MutationSource,
}

impl<'a> MutationSourceFormatter<'a> {
    pub fn create(inner: &'a MutationSource) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(MutationSourceFormatter { inner })
    }
}

impl<'a> PhysicalFormat for MutationSourceFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let table = ctx.metadata.table(self.inner.table_index);
        let table_name = format!("{}.{}.{}", table.catalog(), table.database(), table.name());

        let filters = self
            .inner
            .filters
            .as_ref()
            .map(|filters| filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .unwrap_or_default();

        let mut node_children = vec![
            FormatTreeNode::new(format!("table: {table_name}")),
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, false)
            )),
            FormatTreeNode::new(format!("filters: [{filters}]")),
        ];

        let payload = match self.inner.input_type {
            MutationType::Update => "Update",
            MutationType::Delete if self.inner.truncate_table => "DeleteAll",
            MutationType::Delete => "Delete",
            MutationType::Merge => "Merge",
        };

        // Part stats.
        node_children.extend(part_stats_info_to_format_tree(&self.inner.statistics));
        Ok(FormatTreeNode::with_children(
            format!("MutationSource({})", payload),
            node_children,
        ))
    }

    fn format_join(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let table = ctx.metadata.table(self.inner.table_index).clone();
        let table_name = format!("{}.{}.{}", table.catalog(), table.database(), table.name());
        let mut children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
        if let Some(filters) = &self.inner.filters {
            let filter = filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display();
            children.push(FormatTreeNode::new(format!("filters: [{filter}]")));
        }

        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());
        Ok(FormatTreeNode::with_children(
            "MutationSource".to_string(),
            children,
        ))
    }
}
