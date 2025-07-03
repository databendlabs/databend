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

use std::any::Any;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::format::format_output_columns;
use crate::executor::format::FormatContext;
use crate::executor::physical_plan::DeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::executor::PhysicalPlanMeta;
use crate::plans::CacheSource;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CacheScan {
    pub meta: PhysicalPlanMeta,
    pub cache_source: CacheSource,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for CacheScan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }

    fn to_format_node(
        &self,
        ctx: &mut FormatContext<'_>,
        _: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::with_capacity(2);
        children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.output_schema()?, &ctx.metadata, true)
        )));

        match &self.cache_source {
            CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
                let mut column_indexes = column_indexes.clone();
                column_indexes.sort();
                children.push(FormatTreeNode::new(format!("cache index: {}", cache_index)));
                children.push(FormatTreeNode::new(format!(
                    "column indexes: {:?}",
                    column_indexes
                )));
            }
        }

        Ok(FormatTreeNode::with_children(
            "CacheScan".to_string(),
            children,
        ))
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cache_scan(
        &mut self,
        scan: &crate::plans::CacheScan,
        required: ColumnSet,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        // 1. Prune unused Columns.
        let used: ColumnSet = required.intersection(&scan.columns).cloned().collect();
        let (cache_source, fields) = if used == scan.columns {
            (scan.cache_source.clone(), scan.schema.fields().clone())
        } else {
            let new_scan = scan.prune_columns(used);
            (
                new_scan.cache_source.clone(),
                new_scan.schema.fields().clone(),
            )
        };
        // 2. Build physical plan.
        Ok(Box::new(CacheScan {
            cache_source,
            meta: PhysicalPlanMeta::new("CacheScan"),
            output_schema: DataSchemaRefExt::create(fields),
        }))
    }
}
