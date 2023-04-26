// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use common_ast::ast::ShowLimit;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_expression::Scalar;
use common_pipeline_transforms::processors::transforms::Transform;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::plans::BoundColumnRef;
use common_sql::plans::ConstantExpr;
use common_sql::plans::FunctionCall;
use common_sql::plans::ShowColumnsPlan;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_sql::Planner;
use common_sql::ScalarBinder;
use common_sql::ScalarExpr;
use common_sql::Visibility;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;
use parking_lot::RwLock;
use tracing::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ShowColumnsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowColumnsPlan,
}

impl ShowColumnsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowColumnsPlan) -> Result<Self> {
        Ok(ShowColumnsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowColumnsInterpreter {
    fn name(&self) -> &str {
        "ShowColumnsInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str())?;

        let table = catalog
            .get_table(tenant.as_str(), &self.plan.database, &self.plan.table)
            .await?;

        let engine = table.engine();
        let fields = if engine == VIEW_ENGINE {
            if let Some(query) = table.options().get(QUERY) {
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(query).await?;
                let schema = infer_table_schema(&plan.schema())?;
                schema.fields().clone()
            } else {
                return Err(ErrorCode::Internal(
                    "Logical error, View Table must have a SelectQuery inside.",
                ));
            }
        } else {
            table.schema().fields().clone()
        };

        let fields_len = fields.len();
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut is_nullables: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut exprs: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut extras: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        // full
        let mut collations: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut privileges: Vec<Vec<u8>> = Vec::with_capacity(fields_len);
        let mut comments: Vec<Vec<u8>> = Vec::with_capacity(fields_len);

        // Append columns.
        {
            for field in fields.into_iter() {
                names.push(field.name().clone().into_bytes());
                let data_type = field.data_type().remove_recursive_nullable().sql_name();
                data_types.push(data_type.into_bytes());

                let mut default_expr = "".to_string();
                if let Some(expr) = field.default_expr() {
                    default_expr = expr.to_string();
                }
                exprs.push(default_expr.into_bytes());
                if field.is_nullable() {
                    is_nullables.push("YES".to_string().into_bytes());
                } else {
                    is_nullables.push("NO".to_string().into_bytes());
                }
                extras.push("".to_string().into_bytes());
                keys.push("".to_string().into_bytes());
                if self.plan.full {
                    collations.push("".to_string().into_bytes());
                    privileges.push("".to_string().into_bytes());
                    comments.push("".to_string().into_bytes());
                }
            }
        }

        let block = if self.plan.full {
            DataBlock::new_from_columns(vec![
                // | Field | Type | Collation | Null | Key  | Default | Extra | Privileges | Comment |
                StringType::from_data(names),
                StringType::from_data(data_types),
                StringType::from_data(collations),
                StringType::from_data(is_nullables),
                StringType::from_data(keys),
                StringType::from_data(exprs),
                StringType::from_data(extras),
                StringType::from_data(privileges),
                StringType::from_data(comments),
            ])
        } else {
            // | Field | Type | Null | Key  | Default | Extra |
            DataBlock::new_from_columns(vec![
                StringType::from_data(names),
                StringType::from_data(data_types),
                StringType::from_data(is_nullables),
                StringType::from_data(keys),
                StringType::from_data(exprs),
                StringType::from_data(extras),
            ])
        };

        if let Some(limit) = &self.plan.limit {
            let mut bind_context = BindContext::new();
            let mut column_name = vec![];
            if self.plan.full {
                // | Field | Type | Collation | Null | Key  | Default | Extra | Privileges | Comment |
                column_name.append(&mut vec![
                    "field",
                    "type",
                    "collation",
                    "null",
                    "key",
                    "default",
                    "extra",
                    "privileges",
                    "comment",
                ]);
            } else {
                // | Field | Type | Null | Key  | Default | Extra |
                column_name.append(&mut vec![
                    "field", "type", "null", "key", "default", "extra",
                ]);
            }
            for (index, column_name) in column_name.iter().enumerate() {
                bind_context.add_column_binding(ColumnBinding {
                    database_name: None,
                    table_name: None,
                    table_index: None,
                    column_name: column_name.to_string(),
                    index,
                    data_type: Box::new(DataType::String),
                    visibility: Visibility::Visible,
                });
            }

            let settings = self.ctx.get_settings();
            let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
            let metadata: MetadataRef = Arc::new(RwLock::new(Metadata::default()));
            let mut scalar_binder = ScalarBinder::new(
                &mut bind_context,
                self.ctx.clone(),
                &name_resolution_ctx,
                metadata,
                &[],
            );
            let scalar_expr = match limit {
                ShowLimit::Like { pattern } => Some(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "like".to_string(),
                    params: vec![],
                    arguments: vec![
                        BoundColumnRef {
                            span: None,
                            column: ColumnBinding {
                                database_name: None,
                                table_name: None,
                                table_index: None,
                                column_name: "field".to_string(),
                                index: 0,
                                data_type: Box::new(DataType::String),
                                visibility: Visibility::Visible,
                            },
                        }
                        .into(),
                        ConstantExpr {
                            span: None,
                            value: Scalar::String(pattern.clone().into_bytes()),
                        }
                        .into(),
                    ],
                })),
                ShowLimit::Where { selection } => Some(scalar_binder.bind(selection).await?.0),
            };
            if let Some(scalar_expr) = scalar_expr {
                let mut operators = Vec::with_capacity(6);
                let func_ctx = self.ctx.get_function_context()?;

                let expr = scalar_expr
                    .as_expr_with_col_name()?
                    .project_column_ref(|index| {
                        self.schema().index_of(&index.to_string()).unwrap()
                    });
                operators.push(BlockOperator::Filter { expr });
                let mut expression_transform = CompoundBlockOperator {
                    operators,
                    ctx: func_ctx,
                };
                let res = expression_transform.transform(block)?;
                debug!("Show columns executor result: {:?}", res);
                return PipelineBuildResult::from_blocks(vec![res]);
            }
        }

        debug!("Show columns executor result: {:?}", block);
        PipelineBuildResult::from_blocks(vec![block])
    }
}
