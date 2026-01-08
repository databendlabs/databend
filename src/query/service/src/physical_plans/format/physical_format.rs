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
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_exception::Result;

use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;

pub trait PhysicalFormat {
    #[stacksafe::stacksafe]
    fn dispatch(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut format_node = self.format(ctx)?;

        // explain analyze
        if let Some(prof) = ctx.profs.get(&self.get_meta().plan_id) {
            let mut children = Vec::with_capacity(format_node.children.len() + 10);
            for (_, desc) in get_statistics_desc().iter() {
                if prof.statistics[desc.index] != 0 {
                    children.push(FormatTreeNode::new(format!(
                        "{}: {}",
                        desc.display_name.to_lowercase(),
                        desc.human_format(prof.statistics[desc.index])
                    )));
                }
            }

            children.append(&mut format_node.children);
            format_node.children = children;
        }

        Ok(format_node)
    }

    fn get_meta(&self) -> &PhysicalPlanMeta;

    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>>;

    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>>;

    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>>;
}
