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
use crate::physical_plans::MaterializedCTE;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;

pub struct MaterializedCTEFormatter<'a> {
    inner: &'a MaterializedCTE,
}

impl<'a> MaterializedCTEFormatter<'a> {
    pub fn create(inner: &'a MaterializedCTE) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(MaterializedCTEFormatter { inner })
    }
}

impl<'a> PhysicalFormat for MaterializedCTEFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let input_formatter = self.inner.input.formatter()?;
        let input_payload = input_formatter.dispatch(ctx)?;

        Ok(FormatTreeNode::with_children(
            format!("MaterializedCTE: {}", self.inner.cte_name),
            vec![input_payload],
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let input = self.inner.input.formatter()?.format_join(ctx)?;
        let children = vec![
            FormatTreeNode::new(format!("cte_name: {}", self.inner.cte_name)),
            FormatTreeNode::new(format!("ref_count: {}", self.inner.ref_count)),
            input,
        ];

        Ok(FormatTreeNode::with_children(
            "MaterializedCTE".to_string(),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.partial_format(ctx)
    }
}
