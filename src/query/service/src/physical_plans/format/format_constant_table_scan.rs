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

use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;

pub struct ConstantTableScanFormatter<'a> {
    inner: &'a ConstantTableScan,
}

impl<'a> ConstantTableScanFormatter<'a> {
    pub fn create(inner: &'a ConstantTableScan) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(ConstantTableScanFormatter { inner })
    }
}

impl<'a> PhysicalFormat for ConstantTableScanFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[stacksafe::stacksafe]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.num_rows == 0 {
            return Ok(FormatTreeNode::new(self.inner.name().to_string()));
        }

        let mut children = Vec::with_capacity(self.inner.values.len() + 1);

        let output_schema = self.inner.output_schema()?;
        children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(output_schema, ctx.metadata, true)
        )));
        for (i, value) in self.inner.values.iter().enumerate() {
            let column = value.iter().map(|val| format!("{val}")).join(", ");
            children.push(FormatTreeNode::new(format!("column {}: [{}]", i, column)));
        }

        Ok(FormatTreeNode::with_children(
            self.inner.name().to_string(),
            children,
        ))
    }

    fn format_join(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }

    fn partial_format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }
}
