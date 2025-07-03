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
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;
use itertools::Itertools;

use crate::executor::format::format_output_columns;
use crate::executor::format::FormatContext;
use crate::executor::physical_plan::DeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::executor::PhysicalPlanMeta;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConstantTableScan {
    meta: PhysicalPlanMeta,
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for ConstantTableScan {
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
        if self.num_rows == 0 {
            return Ok(FormatTreeNode::new(self.name().to_string()));
        }

        let mut children = Vec::with_capacity(self.values.len() + 1);
        children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.output_schema()?, &ctx.metadata, true)
        )));
        for (i, value) in self.values.iter().enumerate() {
            let column = value.iter().map(|val| format!("{val}")).join(", ");
            children.push(FormatTreeNode::new(format!("column {}: [{}]", i, column)));
        }

        Ok(FormatTreeNode::with_children(
            self.name().to_string(),
            children,
        ))
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }
}

impl ConstantTableScan {
    pub fn name(&self) -> &str {
        if self.num_rows == 0 {
            "EmptyResultScan"
        } else {
            "ConstantTableScan"
        }
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_constant_table_scan(
        &mut self,
        scan: &crate::plans::ConstantTableScan,
        required: ColumnSet,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        debug_assert!(scan
            .schema
            .fields
            .iter()
            .map(|field| field.name().parse::<IndexType>().unwrap())
            .collect::<ColumnSet>()
            .is_superset(&scan.columns));

        let used: ColumnSet = required.intersection(&scan.columns).copied().collect();
        if used.len() < scan.columns.len() {
            let crate::plans::ConstantTableScan {
                values,
                num_rows,
                schema,
                ..
            } = scan.prune_columns(used);
            return Ok(Box::new(ConstantTableScan {
                values,
                num_rows,
                output_schema: schema,
                meta: PhysicalPlanMeta::new("ConstantTableScan"),
            }));
        }

        Ok(Box::new(ConstantTableScan {
            values: scan.values.clone(),
            num_rows: scan.num_rows,
            output_schema: scan.schema.clone(),
            meta: PhysicalPlanMeta::new("ConstantTableScan"),
        }))
    }
}
