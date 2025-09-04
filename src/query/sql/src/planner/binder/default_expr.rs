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

use std::sync::Arc;

use databend_common_ast::ast::Expr as AExpr;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_functions::BUILTIN_FUNCTIONS;
use parking_lot::RwLock;

use crate::binder::expr_values::ExprValuesRewriter;
use crate::binder::wrap_cast;
use crate::binder::AsyncFunctionDesc;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;
use crate::Metadata;
use crate::MetadataRef;

/// Helper for binding scalar expression with `BindContext`.
pub struct DefaultExprBinder {
    bind_context: BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    name_resolution_ctx: NameResolutionContext,
    metadata: MetadataRef,

    rewriter: ExprValuesRewriter,
    dummy_block: DataBlock,
    func_ctx: FunctionContext,
}

fn get_nextval(s: &ScalarExpr) -> Option<AsyncFunctionCall> {
    match s {
        ScalarExpr::AsyncFunctionCall(c) => {
            if c.func_name == "nextval" {
                Some(c.clone())
            } else {
                None
            }
        }
        ScalarExpr::CastExpr(c) => get_nextval(&c.argument),
        _ => None,
    }
}

impl DefaultExprBinder {
    pub fn try_new(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
        let bind_context = BindContext::new();
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let metadata = Arc::new(RwLock::new(Metadata::default()));

        let dummy_block = DataBlock::new(vec![], 1);
        let func_ctx = ctx.get_function_context()?;
        let rewriter = ExprValuesRewriter::new(ctx.clone(), true);

        Ok(DefaultExprBinder {
            bind_context,
            ctx,
            dialect,
            name_resolution_ctx,
            metadata,
            rewriter,
            dummy_block,
            func_ctx,
        })
    }

    fn evaluator(&self) -> Evaluator {
        Evaluator::new(&self.dummy_block, &self.func_ctx, &BUILTIN_FUNCTIONS)
    }

    pub fn parse_default_expr_to_string(
        &mut self,
        field: &TableField,
        ast: &AExpr,
    ) -> Result<(String, bool)> {
        let data_field: DataField = field.into();
        let scalar_expr = self.bind(ast, data_field.data_type())?;
        let is_nextval = get_nextval(&scalar_expr).is_some();
        if !scalar_expr.evaluable() && !is_nextval {
            return Err(ErrorCode::SemanticError(format!(
                "default value expression `{:#}` is invalid",
                ast
            )));
        }
        let expr = scalar_expr.as_expr()?;
        let (expr, is_deterministic) = if is_nextval {
            (expr, false)
        } else if expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            let (fold_to_constant, _) =
                ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            (fold_to_constant, true)
        } else {
            (expr, false)
        };
        Ok((expr.sql_display(), is_deterministic))
    }

    pub fn parse(&mut self, default_expr: &str) -> Result<AExpr> {
        let tokens = tokenize_sql(default_expr)?;
        let ast = parse_expr(&tokens, self.dialect)?;
        Ok(ast)
    }

    pub fn bind(&mut self, ast: &AExpr, dest_type: &DataType) -> Result<ScalarExpr> {
        let mut type_checker = TypeChecker::try_create(
            &mut self.bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            true,
        )?;
        let (mut scalar, data_type) = *type_checker.resolve(ast)?;
        if &data_type != dest_type {
            scalar = wrap_cast(&scalar, dest_type);
        }
        Ok(scalar)
    }

    pub fn parse_and_bind(&mut self, field: &DataField) -> Result<ScalarExpr> {
        if let Some(default_expr) = field.default_expr() {
            let ast = self.parse(default_expr).map_err(|e| {
                format!(
                    "fail to parse default expr `{}` (string length = {}) of field {}, {}",
                    default_expr,
                    default_expr.len(),
                    field.name(),
                    e
                )
            })?;
            self.bind(&ast, field.data_type())
        } else {
            Ok(ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: Scalar::default_value(field.data_type()),
            }))
        }
    }

    pub fn get_scalar(&mut self, field: &TableField) -> Result<Scalar> {
        let data_field: DataField = field.into();
        let mut scalar_expr = self.parse_and_bind(&data_field)?;
        self.rewriter.visit(&mut scalar_expr)?;
        let expr = scalar_expr
            .as_expr()?
            .project_column_ref(|col| Ok(col.index))?;
        let result = self.evaluator().run(&expr)?;
        match result {
            databend_common_expression::Value::Scalar(s) => Ok(s),
            databend_common_expression::Value::Column(c) if c.len() == 1 => {
                let value = unsafe { c.index_unchecked(0) };
                Ok(value.to_owned())
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "Invalid default value for column: {}, must be constant, but got: {}",
                field.name(),
                result
            ))),
        }
    }

    pub fn get_expr(
        &mut self,
        field: &DataField,
        schema: &DataSchema,
    ) -> Result<databend_common_expression::Expr> {
        let scalar_expr = self.parse_and_bind(field)?;
        scalar_expr
            .as_expr()?
            .project_column_ref(|col| schema.index_of(&col.index.to_string()))
    }

    pub fn prepare_default_values(
        &mut self,
        data_schema: &DataSchemaRef,
    ) -> Result<Vec<RemoteDefaultExpr>> {
        let mut values = Vec::with_capacity(data_schema.fields.len());
        for field in &data_schema.fields {
            let scalar_expr = self.parse_and_bind(field)?;
            let expr = if let Some(async_func) = get_nextval(&scalar_expr) {
                let name = match async_func.func_arg {
                    AsyncFunctionArgument::SequenceFunction(name) => name,
                    AsyncFunctionArgument::DictGetFunction(_) => {
                        unreachable!("expect AsyncFunctionArgument::SequenceFunction")
                    }
                };
                RemoteDefaultExpr::Sequence(name)
            } else {
                let expr = scalar_expr
                    .as_expr()?
                    .project_column_ref(|col| data_schema.index_of(&col.index.to_string()))?;
                RemoteDefaultExpr::RemoteExpr(expr.as_remote_expr())
            };
            values.push(expr);
        }
        Ok(values)
    }

    pub fn split_async_default_exprs(
        &mut self,
        input_schema: DataSchemaRef,
        dest_schema: DataSchemaRef,
    ) -> Result<Option<(Vec<AsyncFunctionDesc>, DataSchemaRef, DataSchemaRef)>> {
        let mut async_func_descs = vec![];
        let mut async_fields = vec![];
        let mut async_fields_no_cast = vec![];

        for f in dest_schema.fields().iter() {
            if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    if default_expr.contains("nextval(") {
                        let scalar_expr = self.parse_and_bind(f)?;
                        if let Some(async_func) = get_nextval(&scalar_expr) {
                            async_func_descs.push(AsyncFunctionDesc {
                                func_name: async_func.func_name.clone(),
                                display_name: async_func.display_name.clone(),
                                // not used
                                output_column: 0,
                                arg_indices: vec![],
                                data_type: async_func.return_type.clone(),
                                func_arg: async_func.func_arg.clone(),
                            });
                            async_fields.push(f.clone());
                            async_fields_no_cast.push(DataField::new(
                                f.name(),
                                DataType::Number(NumberDataType::UInt64),
                            ));
                        }
                    }
                }
            };
        }
        if async_func_descs.is_empty() {
            Ok(None)
        } else {
            let mut fields = input_schema.fields().to_vec();
            fields.extend_from_slice(&async_fields_no_cast);
            let schema_no_cast = Arc::new(DataSchema::new(fields));

            let mut fields = input_schema.fields().to_vec();
            fields.extend_from_slice(&async_fields);
            let schema = Arc::new(DataSchema::new(fields));
            Ok(Some((async_func_descs, schema, schema_no_cast)))
        }
    }
}
