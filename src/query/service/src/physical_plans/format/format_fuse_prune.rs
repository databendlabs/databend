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

use crate::physical_plans::FusePrune;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::part_stats_info_to_format_tree;

pub struct FusePruneFormatter<'a> {
    inner: &'a FusePrune,
}

impl<'a> FusePruneFormatter<'a> {
    pub fn create(inner: &'a FusePrune) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(FusePruneFormatter { inner })
    }
}

impl PhysicalFormat for FusePruneFormatter<'_> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let table_name = ctx
            .metadata
            .table(self.inner.source.table_index)
            .qualified_name();
        let mut children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
        children.extend(part_stats_info_to_format_tree(
            &self.inner.source.statistics,
        ));

        Ok(FormatTreeNode::with_children(
            "FusePrune".to_string(),
            children,
        ))
    }

    fn format_join(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::new(self.inner.get_name()))
    }

    fn partial_format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::new(self.inner.get_name()))
    }
}
