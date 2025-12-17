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
use std::collections::HashMap;

use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamColumnType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_sql::Binder;
use databend_common_sql::CURRENT_BLOCK_ID_COL_NAME;
use databend_common_sql::CURRENT_BLOCK_ROW_NUM_COL_NAME;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_sql::StreamContext;
use databend_common_sql::Visibility;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;

use crate::physical_plans::format::AddStreamColumnFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddStreamColumn {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub exprs: Vec<RemoteExpr>,
    pub projections: Vec<usize>,
    pub stream_columns: Vec<StreamColumn>,
}

impl IPhysicalPlan for AddStreamColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(AddStreamColumnFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.remove(0);

        PhysicalPlan::new(AddStreamColumn {
            input,
            meta: self.meta.clone(),
            exprs: self.exprs.clone(),
            projections: self.projections.clone(),
            stream_columns: self.stream_columns.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let exprs = self
            .exprs
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        let operators = vec![
            BlockOperator::Map {
                exprs,
                projections: None,
            },
            BlockOperator::Project {
                projection: self.projections.clone(),
            },
        ];

        let stream_context = StreamContext {
            stream_columns: self.stream_columns.clone(),
            operators,
            func_ctx: builder.ctx.get_function_context()?,
        };

        builder
            .main_pipeline
            .add_transformer(|| TransformAddStreamColumns::new(stream_context.clone()));

        Ok(())
    }
}

impl AddStreamColumn {
    pub fn create(
        metadata: &MetadataRef,
        input: PhysicalPlan,
        table_index: usize,
        table_version: u64,
    ) -> Result<PhysicalPlan> {
        let input_schema = input.output_schema()?;
        let num_fields = input_schema.fields().len();
        let column_entries = metadata.read().columns_by_table_index(table_index);

        let stream_columns = [
            StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion),
            StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId),
            StreamColumn::new(
                ORIGIN_BLOCK_ROW_NUM_COL_NAME,
                StreamColumnType::OriginRowNum,
            ),
        ];

        let mut new_schema_index = HashMap::with_capacity(stream_columns.len());
        let mut exprs = Vec::with_capacity(stream_columns.len());
        for stream_column in stream_columns.iter() {
            let column_index =
                Binder::find_column_index(&column_entries, stream_column.column_name())?;
            let schema_index = input_schema.index_of(&column_index.to_string()).unwrap();

            let origin_stream_column_scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    stream_column.column_name().to_string(),
                    schema_index,
                    Box::new(stream_column.data_type()),
                    Visibility::Visible,
                )
                .build(),
            });

            let current_stream_column_scalar_expr = match stream_column.column_type() {
                StreamColumnType::OriginVersion => {
                    new_schema_index.insert(schema_index, num_fields + 2);
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: table_version.into(),
                    })
                }
                StreamColumnType::OriginBlockId => {
                    new_schema_index.insert(schema_index, num_fields + 3);
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            CURRENT_BLOCK_ID_COL_NAME.to_string(),
                            num_fields + 1,
                            Box::new(stream_column.data_type()),
                            Visibility::Visible,
                        )
                        .build(),
                    })
                }
                StreamColumnType::OriginRowNum => {
                    new_schema_index.insert(schema_index, num_fields + 4);
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            CURRENT_BLOCK_ROW_NUM_COL_NAME.to_string(),
                            num_fields,
                            Box::new(stream_column.data_type()),
                            Visibility::Visible,
                        )
                        .build(),
                    })
                }
            };

            let new_stream_column_scalar_expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "if".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "is_not_null".to_string(),
                        params: vec![],
                        arguments: vec![origin_stream_column_scalar_expr.clone()],
                    }),
                    origin_stream_column_scalar_expr,
                    current_stream_column_scalar_expr,
                ],
            });

            exprs.push(
                new_stream_column_scalar_expr
                    .as_expr()?
                    .project_column_ref(|col| Ok(col.index))?
                    .as_remote_expr(),
            );
        }

        // Add projection to keep input schema.
        let mut projections = Vec::with_capacity(num_fields);
        for i in 0..num_fields {
            if let Some(index) = new_schema_index.get(&i) {
                projections.push(*index);
            } else {
                projections.push(i);
            }
        }

        // ORIGIN_BLOCK_ROW_NUM, ORIGIN_BLOCK_ID.
        let stream_columns = vec![stream_columns[2].clone(), stream_columns[1].clone()];

        Ok(PhysicalPlan::new(AddStreamColumn {
            input,
            exprs,
            projections,
            stream_columns,
            meta: PhysicalPlanMeta::new("AddStreamColumn"),
        }))
    }
}
