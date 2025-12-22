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

use crate::physical_plans::ExchangeSource;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;

pub struct ExchangeSourceFormatter<'a> {
    inner: &'a ExchangeSource,
}

impl<'a> ExchangeSourceFormatter<'a> {
    pub fn create(inner: &'a ExchangeSource) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(ExchangeSourceFormatter { inner })
    }
}

impl<'a> PhysicalFormat for ExchangeSourceFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let output_schema = self.inner.output_schema()?;
        let mut node_children = vec![FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(output_schema, ctx.metadata, true)
        ))];

        node_children.push(FormatTreeNode::new(format!(
            "source fragment: [{}]",
            self.inner.source_fragment_id
        )));

        Ok(FormatTreeNode::with_children(
            "ExchangeSource".to_string(),
            node_children,
        ))
    }

    fn format_join(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }

    fn partial_format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }
}
