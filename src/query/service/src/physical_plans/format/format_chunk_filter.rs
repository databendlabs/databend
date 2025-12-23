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

use crate::physical_plans::ChunkFilter;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;

pub struct ChunkFilterFormatter<'a> {
    inner: &'a ChunkFilter,
}

impl<'a> ChunkFilterFormatter<'a> {
    pub fn create(inner: &'a ChunkFilter) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(ChunkFilterFormatter { inner })
    }
}

impl<'a> PhysicalFormat for ChunkFilterFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.inner.predicates.iter().all(|x| x.is_none()) {
            let input_formatter = self.inner.input.formatter()?;
            return input_formatter.dispatch(ctx);
        }

        let mut node_children = Vec::new();
        for (i, predicate) in self.inner.predicates.iter().enumerate() {
            if let Some(predicate) = predicate {
                node_children.push(FormatTreeNode::new(format!(
                    "branch {}: {}",
                    i,
                    predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                )));
            } else {
                node_children.push(FormatTreeNode::new(format!("branch {}: None", i)));
            }
        }

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "Filter".to_string(),
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
