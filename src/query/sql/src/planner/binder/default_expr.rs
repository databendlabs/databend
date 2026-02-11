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
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_meta_types::MetaId;
use parking_lot::RwLock;

use crate::Metadata;
use crate::MetadataRef;
use crate::binder::AsyncFunctionDesc;
use crate::binder::wrap_cast;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

/// Helper for binding scalar expression with `BindContext`.
pub struct DefaultExprBinder {
    // the table id of the auto increment column processed by the binder
    auto_increment_table_id: Option<MetaId>,
    bind_context: BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    name_resolution_ctx: NameResolutionContext,
    metadata: MetadataRef,

    rewriter: DefaultValueRewriter,
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
        let rewriter = DefaultValueRewriter::new();

        Ok(DefaultExprBinder {
            auto_increment_table_id: None,
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

    pub fn auto_increment_table_id(mut self, table_id: u64) -> Self {
        self.auto_increment_table_id = Some(table_id);
        self
    }

    fn evaluator(&self) -> Evaluator<'_> {
        Evaluator::new(&self.dummy_block, &self.func_ctx, &BUILTIN_FUNCTIONS)
    }

    pub fn parse_default_expr_to_string(
        &mut self,
        field: &TableField,
        ast: &AExpr,
    ) -> Result<(String, bool, bool)> {
        let data_field: DataField = field.into();
        let (scalar_expr, data_type) = self.bind(ast)?;
        let (evaluable, is_nextval) = scalar_expr.default_value_evaluable();
        // The nextval can not work with other expressions.
        if !evaluable || (is_nextval && !matches!(scalar_expr, ScalarExpr::AsyncFunctionCall(_))) {
            return Err(ErrorCode::SemanticError(format!(
                "default value expression `{:#}` is invalid",
                ast
            )));
        }
        let dest_type = data_field.data_type();
        // The data type of nextval must be number or decimal.
        if is_nextval
            && !matches!(
                dest_type.remove_nullable(),
                DataType::Number(_) | DataType::Decimal(_)
            )
        {
            return Err(ErrorCode::SemanticError(format!(
                "default data type does not match data type for column {}",
                data_field.name()
            )));
        }

        let scalar_expr = if data_type != *dest_type {
            wrap_cast(&scalar_expr, dest_type)
        } else {
            scalar_expr
        };
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
        Ok((expr.sql_display(), is_deterministic, is_nextval))
    }

    pub fn parse(&mut self, default_expr: &str) -> Result<AExpr> {
        let tokens = tokenize_sql(default_expr)?;
        let ast = parse_expr(&tokens, self.dialect)?;
        Ok(ast)
    }

    fn bind_impl(
        &mut self,
        ast: &AExpr,
        skip_sequence_check: bool,
    ) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::try_create(
            &mut self.bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            true,
        )?;
        if skip_sequence_check {
            type_checker.set_skip_sequence_check(true);
        }
        let (scalar_expr, data_type) = *type_checker.resolve(ast)?;
        Ok((scalar_expr, data_type))
    }

    pub fn bind(&mut self, ast: &AExpr) -> Result<(ScalarExpr, DataType)> {
        self.bind_impl(ast, false)
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
            let (scalar_expr, data_type) = self.bind_impl(&ast, true)?;
            let dest_type = field.data_type();
            if data_type != *dest_type {
                Ok(wrap_cast(&scalar_expr, dest_type))
            } else {
                Ok(scalar_expr)
            }
        } else if let (Some(table_id), Some(auto_increment_expr)) =
            (&self.auto_increment_table_id, field.auto_increment_expr())
        {
            let auto_increment_key =
                AutoIncrementKey::new(*table_id, auto_increment_expr.column_id);

            Ok(ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span: None,
                func_name: "nextval".to_string(),
                display_name: "".to_string(),
                return_type: Box::new(field.data_type().clone()),
                arguments: vec![],
                func_arg: AsyncFunctionArgument::AutoIncrement {
                    key: auto_increment_key,
                    expr: auto_increment_expr.clone(),
                },
            }))
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
                    AsyncFunctionArgument::AutoIncrement { .. } => {
                        unreachable!("expect AsyncFunctionArgument::SequenceFunction")
                    }
                    AsyncFunctionArgument::DictGetFunction(_) => {
                        unreachable!("expect AsyncFunctionArgument::SequenceFunction")
                    }
                    AsyncFunctionArgument::ReadFile(_) => {
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

        let fn_check_auto_increment = |field: &DataField| {
            if input_schema.has_field(field.name()) {
                return false;
            }
            if let Some(default_expr) = field.default_expr() {
                if default_expr.contains("nextval(") {
                    return true;
                }
            }
            field.auto_increment_expr().is_some()
        };
        for f in dest_schema.fields().iter() {
            if !fn_check_auto_increment(f) {
                continue;
            }
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

struct DefaultValueRewriter {}

impl DefaultValueRewriter {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'a> VisitorMut<'a> for DefaultValueRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::AsyncFunctionCall(async_func) = &expr {
            if async_func.func_name == "nextval" {
                // Don't generate a new sequence next value,
                // because the default value is not used for new inserted values.
                *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                    span: async_func.span,
                    value: Scalar::default_value(&async_func.return_type),
                });
            }
        }
        walk_expr_mut(self, expr)
    }
}
