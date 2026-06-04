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
use crate::physical_plans::Window;
use crate::physical_plans::WindowFunction;
use crate::physical_plans::WindowGroup;
use crate::physical_plans::WindowSpec;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::pretty_display_agg_desc;

pub struct WindowFormatter<'a> {
    inner: &'a Window,
}

impl<'a> WindowFormatter<'a> {
    pub fn create(inner: &'a Window) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(WindowFormatter { inner })
    }
}

pub struct WindowGroupFormatter<'a> {
    inner: &'a WindowGroup,
}

impl<'a> WindowGroupFormatter<'a> {
    pub fn create(inner: &'a WindowGroup) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(WindowGroupFormatter { inner })
    }
}

impl<'a> PhysicalFormat for WindowFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let partition_by = self
            .inner
            .partition_by
            .iter()
            .map(|&index| Ok(ctx.metadata.column(index).name()))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let order_by = self
            .inner
            .order_by
            .iter()
            .map(|v| v.display_name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let frame = self.inner.window_frame.to_string();
        let func = match &self.inner.func {
            WindowFunction::Aggregate(agg) => pretty_display_agg_desc(agg, ctx.metadata),
            func => format!("{}", func),
        };

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("aggregate function: [{}]", func)),
            FormatTreeNode::new(format!("partition by: [{}]", partition_by)),
            FormatTreeNode::new(format!("order by: [{}]", order_by)),
            FormatTreeNode::new(format!("frame: [{}]", frame)),
        ];

        if let Some(limit) = self.inner.limit {
            node_children.push(FormatTreeNode::new(format!("limit: [{}]", limit)));
        }

        // Add child nodes at the end (format input)
        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "Window".to_string(),
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

impl<'a> PhysicalFormat for WindowGroupFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.windows.len() == 1 {
            let mut node_children = format_window_spec_children(&self.inner.windows[0], ctx)?;
            node_children.insert(
                0,
                FormatTreeNode::new(format!(
                    "output columns: [{}]",
                    format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
                )),
            );

            let input_formatter = self.inner.input.formatter()?;
            node_children.push(input_formatter.dispatch(ctx)?);

            return Ok(FormatTreeNode::with_children(
                "Window".to_string(),
                node_children,
            ));
        }

        let mut node_children = vec![FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
        ))];

        for window in &self.inner.windows {
            node_children.push(format_window_spec(window, ctx)?);
        }

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "WindowGroup".to_string(),
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

fn format_window_spec(
    window: &WindowSpec,
    ctx: &mut FormatContext<'_>,
) -> Result<FormatTreeNode<String>> {
    let node_children = format_window_spec_children(window, ctx)?;
    Ok(FormatTreeNode::with_children(
        "Window".to_string(),
        node_children,
    ))
}

fn format_window_spec_children(
    window: &WindowSpec,
    ctx: &mut FormatContext<'_>,
) -> Result<Vec<FormatTreeNode<String>>> {
    let partition_by = window
        .partition_by
        .iter()
        .map(|&index| Ok(ctx.metadata.column(index).name()))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let order_by = window
        .order_by
        .iter()
        .map(|v| v.display_name.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let frame = window.window_frame.to_string();
    let func = match &window.func {
        WindowFunction::Aggregate(agg) => pretty_display_agg_desc(agg, ctx.metadata),
        func => format!("{}", func),
    };

    let mut node_children = vec![
        FormatTreeNode::new(format!("output column: [{}]", window.index)),
        FormatTreeNode::new(format!("aggregate function: [{}]", func)),
        FormatTreeNode::new(format!("partition by: [{}]", partition_by)),
        FormatTreeNode::new(format!("order by: [{}]", order_by)),
        FormatTreeNode::new(format!("frame: [{}]", frame)),
    ];

    if let Some(limit) = window.limit {
        node_children.push(FormatTreeNode::new(format!("limit: [{}]", limit)));
    }
    if let Some(top) = window.top {
        node_children.push(FormatTreeNode::new(format!("top: [{}]", top)));
    }

    Ok(node_children)
}
