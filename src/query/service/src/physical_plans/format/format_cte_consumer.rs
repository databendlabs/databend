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

use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MaterializeCTERef;
use crate::physical_plans::PhysicalPlanMeta;

pub struct MaterializeCTERefFormatter<'a> {
    inner: &'a MaterializeCTERef,
}

impl<'a> MaterializeCTERefFormatter<'a> {
    pub fn create(inner: &'a MaterializeCTERef) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(MaterializeCTERefFormatter { inner })
    }
}

impl<'a> PhysicalFormat for MaterializeCTERefFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::new();
        children.push(FormatTreeNode::new(format!(
            "cte_name: {}",
            self.inner.cte_name.clone()
        )));
        children.push(FormatTreeNode::new(format!(
            "cte_schema: [{}]",
            format_output_columns(self.inner.cte_schema.clone(), ctx.metadata, false)
        )));

        if let Some(info) = &self.inner.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            children.extend(items);
        }

        Ok(FormatTreeNode::with_children(
            "MaterializeCTERef".to_string(),
            children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let children = vec![
            FormatTreeNode::new(format!("cte_name: {}", self.inner.cte_name)),
            FormatTreeNode::new(format!(
                "cte_schema: [{}]",
                format_output_columns(self.inner.cte_schema.clone(), ctx.metadata, false)
            )),
        ];
        Ok(FormatTreeNode::with_children(
            "MaterializeCTERef".to_string(),
            children,
        ))
    }

    fn partial_format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }
}
