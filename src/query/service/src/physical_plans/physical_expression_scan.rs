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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::ColumnSet;
use databend_common_sql::TypeCheck;
use itertools::Itertools;

use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::processors::transforms::TransformExpressionScan;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExpressionScan {
    pub meta: PhysicalPlanMeta,
    pub values: Vec<Vec<RemoteExpr>>,
    pub input: Box<dyn IPhysicalPlan>,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for ExpressionScan {
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

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let mut node_children = Vec::with_capacity(self.values.len() + 1);
        node_children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.output_schema()?, &ctx.metadata, true)
        )));

        for (i, value) in self.values.iter().enumerate() {
            let column = value
                .iter()
                .map(|val| val.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", ");
            node_children.push(FormatTreeNode::new(format!("column {}: [{}]", i, column)));
        }

        node_children.extend(children);

        Ok(FormatTreeNode::with_children(
            "ExpressionScan".to_string(),
            node_children,
        ))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let values = self
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let fun_ctx = builder.func_ctx.clone();

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformExpressionScan::create(
                input,
                output,
                values.clone(),
                fun_ctx.clone(),
            )))
        })?;

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_expression_scan(
        &mut self,
        s_expr: &SExpr,
        scan: &databend_common_sql::plans::ExpressionScan,
        required: ColumnSet,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;

        let values = scan
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|scalar| {
                        let expr = scalar
                            .type_check(input_schema.as_ref())?
                            .project_column_ref(|index| {
                                input_schema.index_of(&index.to_string()).unwrap()
                            });
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        Ok(expr.as_remote_expr())
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(ExpressionScan {
            values,
            input,
            output_schema: scan.schema.clone(),
            meta: PhysicalPlanMeta::new("ExpressionScan"),
        }))
    }
}
