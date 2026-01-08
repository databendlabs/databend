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

use crate::physical_plans::ExchangeSink;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;

pub struct ExchangeSinkFormatter<'a> {
    inner: &'a ExchangeSink,
}

impl<'a> ExchangeSinkFormatter<'a> {
    pub fn create(inner: &'a ExchangeSink) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(ExchangeSinkFormatter { inner })
    }
}

impl<'a> PhysicalFormat for ExchangeSinkFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[stacksafe::stacksafe]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let output_schema = self.inner.output_schema()?;
        let mut node_children = vec![FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(output_schema, ctx.metadata, true)
        ))];

        node_children.push(FormatTreeNode::new(format!(
            "destination fragment: [{}]",
            self.inner.destination_fragment_id
        )));

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "ExchangeSink".to_string(),
            node_children,
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
