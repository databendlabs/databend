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

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::PhysicalSpatialJoin;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::append_output_rows_info;
use crate::physical_plans::format::format_output_columns;

pub struct SpatialJoinFormatter<'a> {
    inner: &'a PhysicalSpatialJoin,
}

impl<'a> SpatialJoinFormatter<'a> {
    pub fn create(inner: &'a PhysicalSpatialJoin) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(SpatialJoinFormatter { inner })
    }
}

impl<'a> PhysicalFormat for SpatialJoinFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let predicates = self
            .inner
            .predicates
            .iter()
            .map(|p| p.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(", ");

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("build side: {:?}", self.inner.build_side)),
            FormatTreeNode::new(format!("spatial predicates: [{predicates}]")),
        ];

        let build_formatter = self.inner.build.formatter()?;
        let mut build_child = build_formatter.dispatch(ctx)?;
        build_child.payload = format!("{}(Build)", build_child.payload);

        let probe_formatter = self.inner.probe.formatter()?;
        let mut probe_child = probe_formatter.dispatch(ctx)?;
        probe_child.payload = format!("{}(Probe)", probe_child.payload);

        node_children.push(build_child);
        node_children.push(probe_child);

        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let build_child = self.inner.build.formatter()?.format_join(ctx)?;
        let probe_child = self.inner.probe.formatter()?.format_join(ctx)?;

        let children = vec![
            FormatTreeNode::with_children("Build".to_string(), vec![build_child]),
            FormatTreeNode::with_children("Probe".to_string(), vec![probe_child]),
        ];

        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let build_child = self.inner.build.formatter()?.partial_format(ctx)?;
        let probe_child = self.inner.probe.formatter()?.partial_format(ctx)?;

        let mut children = vec![];
        append_output_rows_info(&mut children, &ctx.profs, self.inner.get_id());

        children.push(build_child);
        children.push(probe_child);

        Ok(FormatTreeNode::with_children(
            self.inner.get_name(),
            children,
        ))
    }
}
