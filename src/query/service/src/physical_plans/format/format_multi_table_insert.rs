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

use crate::physical_plans::ChunkAppendData;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;

pub struct ChunkAppendDataFormatter<'a> {
    inner: &'a ChunkAppendData,
}

impl<'a> ChunkAppendDataFormatter<'a> {
    pub fn create(inner: &'a ChunkAppendData) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(ChunkAppendDataFormatter { inner })
    }
}

impl<'a> PhysicalFormat for ChunkAppendDataFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[stacksafe::stacksafe]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let input_formatter = self.inner.input.formatter()?;
        let input_payload = input_formatter.dispatch(ctx)?;

        Ok(FormatTreeNode::with_children(
            "WriteData".to_string(),
            vec![input_payload],
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
