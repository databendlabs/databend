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

use common_ast::ast::Expr as AExpr;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::walk_expr_mut;
use common_ast::Dialect;
use common_base::base::tokio::runtime::Handle;
use common_base::base::tokio::task::block_in_place;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_schema_type;
use common_expression::infer_table_schema;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;
use common_settings::Settings;
use parking_lot::RwLock;

use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::CastExpr;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::IdentifierNormalizer;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Visibility;

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Vec<Expr>> {
    let settings = Settings::create("".to_string());
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
        false,
        false,
    );

    let columns = metadata.read().columns_by_table_index(table_index);
    let table = metadata.read().table(table_index).clone();
    for (index, column) in columns.iter().enumerate() {
        let column_binding = match column {
            ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_name,
                data_type,
                path_indices,
                virtual_computed_expr,
                ..
            }) => {
                let visibility = if path_indices.is_some() {
                    Visibility::InVisible
                } else {
                    Visibility::Visible
                };
                ColumnBindingBuilder::new(
                    column_name.clone(),
                    index,
                    Box::new(data_type.into()),
                    visibility,
                )
                .database_name(Some("default".to_string()))
                .table_name(Some(table.name().to_string()))
                .table_index(Some(table.index()))
                .virtual_computed_expr(virtual_computed_expr.clone())
                .build()
            }
            _ => {
                return Err(ErrorCode::Internal("Invalid column entry"));
            }
        };

        bind_context.add_column_binding(column_binding);
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
        false,
    );

    let sql_dialect = Dialect::MySQL;
    let tokens = tokenize_sql(sql)?;
    let ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    let exprs = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) =
                *block_in_place(|| Handle::current().block_on(type_checker.resolve(ast)))?;
            let expr = scalar.as_expr()?.project_column_ref(|col| col.index);
            Ok(expr)
        })
        .collect::<Result<_>>()?;

    Ok(exprs)
}

pub fn parse_to_remote_string_expr(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<RemoteExpr<String>> {
    let schema = table_meta.schema();
    let exprs = parse_exprs(ctx, table_meta, sql)?;
    let exprs: Vec<RemoteExpr<String>> = exprs
        .iter()
        .map(|expr| {
            expr.project_column_ref(|index| schema.field(*index).name().to_string())
                .as_remote_expr()
        })
        .collect();

    if exprs.len() == 1 {
        Ok(exprs[0].clone())
    } else {
        Err(ErrorCode::BadDataValueType(format!(
            "Expected single expr, but got {}",
            exprs.len()
        )))
    }
}

pub fn parse_computed_expr(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    sql: &str,
) -> Result<Expr> {
    let settings = Settings::create("".to_string());
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();
    let table_schema = infer_table_schema(&schema)?;
    for (index, field) in schema.fields().iter().enumerate() {
        let column = ColumnBindingBuilder::new(
            field.name().clone(),
            index,
            Box::new(field.data_type().clone()),
            Visibility::Visible,
        )
        .build();
        bind_context.add_column_binding(column);
        let table_field = table_schema.field(index);
        metadata.add_base_table_column(
            table_field.name().clone(),
            table_field.data_type().clone(),
            0,
            None,
            None,
            None,
            None,
        );
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
        false,
    );

    let sql_dialect = Dialect::PostgreSQL;
    let tokens = tokenize_sql(sql)?;
    let mut asts = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    if asts.len() != 1 {
        return Err(ErrorCode::BadDataValueType(format!(
            "Expected single expr, but got {}",
            asts.len()
        )));
    }
    let ast = asts.remove(0);
    let (scalar, _) = *block_in_place(|| Handle::current().block_on(type_checker.resolve(&ast)))?;
    let expr = scalar.as_expr()?.project_column_ref(|col| col.index);
    Ok(expr)
}

pub fn parse_default_expr_to_string(
    ctx: Arc<dyn TableContext>,
    field: &TableField,
    ast: &AExpr,
    is_add_column: bool,
) -> Result<String> {
    let settings = Settings::create("".to_string());
    let mut bind_context = BindContext::new();
    let metadata = Metadata::default();

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
        false,
    );

    let (mut scalar, data_type) =
        *block_in_place(|| Handle::current().block_on(type_checker.resolve(ast)))?;
    let schema_data_type = DataType::from(field.data_type());
    let is_try = schema_data_type.is_nullable();
    if data_type != schema_data_type {
        scalar = ScalarExpr::CastExpr(CastExpr {
            span: ast.span(),
            is_try,
            target_type: Box::new(schema_data_type),
            argument: Box::new(scalar.clone()),
        });
    }
    let expr = scalar.as_expr()?;

    // Added columns are not allowed to use expressions,
    // as the default values will be generated at at each query.
    if is_add_column && !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
        return Err(ErrorCode::SemanticError(format!(
            "default expression `{}` is not a valid constant. Please provide a valid constant expression as the default value.",
            expr.sql_display(),
        )));
    }
    let expr = if expr.is_deterministic(&BUILTIN_FUNCTIONS) {
        let (fold_to_constant, _) =
            ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
        fold_to_constant
    } else {
        expr
    };
    Ok(expr.sql_display())
}

pub fn parse_computed_expr_to_string(
    ctx: Arc<dyn TableContext>,
    table_schema: TableSchemaRef,
    field: &TableField,
    ast: &AExpr,
) -> Result<String> {
    let settings = Settings::create("".to_string());
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();
    for (index, field) in table_schema.fields().iter().enumerate() {
        bind_context.add_column_binding(
            ColumnBindingBuilder::new(
                field.name().clone(),
                index,
                Box::new(field.data_type().into()),
                Visibility::Visible,
            )
            .build(),
        );
        metadata.add_base_table_column(
            field.name().clone(),
            field.data_type().clone(),
            0,
            None,
            None,
            None,
            None,
        );
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
        false,
    );

    let (scalar, data_type) =
        *block_in_place(|| Handle::current().block_on(type_checker.resolve(ast)))?;
    if data_type != DataType::from(field.data_type()) {
        return Err(ErrorCode::SemanticError(format!(
            "expected computed column expression have type {}, but `{}` has type {}.",
            field.data_type(),
            ast,
            data_type,
        )));
    }
    let computed_expr = scalar.as_expr()?;
    if !computed_expr.is_deterministic(&BUILTIN_FUNCTIONS) {
        return Err(ErrorCode::SemanticError(format!(
            "computed column expression `{}` is not deterministic.",
            computed_expr.sql_display(),
        )));
    }
    let mut ast = ast.clone();
    walk_expr_mut(
        &mut IdentifierNormalizer {
            ctx: &name_resolution_ctx,
        },
        &mut ast,
    );
    Ok(format!("{:#}", ast))
}

pub fn parse_lambda_expr(
    ctx: Arc<dyn TableContext>,
    column_name: &str,
    data_type: &DataType,
    ast: &AExpr,
) -> Result<Box<(ScalarExpr, DataType)>> {
    let settings = Settings::create("".to_string());
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();

    bind_context.set_expr_context(ExprContext::InLambdaFunction);
    bind_context.add_column_binding(
        ColumnBindingBuilder::new(
            column_name.to_string(),
            0,
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build(),
    );

    let table_type = infer_schema_type(data_type)?;
    metadata.add_base_table_column(
        column_name.to_string(),
        table_type,
        0,
        None,
        None,
        None,
        None,
    );

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
        false,
    );

    block_in_place(|| Handle::current().block_on(type_checker.resolve(ast)))
}

#[derive(Default)]
struct DummyTable {
    info: TableInfo,
}
impl Table for DummyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_table_info(&self) -> &common_meta_app::schema::TableInfo {
        &self.info
    }
}

pub fn field_default_value(ctx: Arc<dyn TableContext>, field: &TableField) -> Result<Scalar> {
    let data_type = field.data_type();
    let data_type = DataType::from(data_type);

    match field.default_expr() {
        Some(default_expr) => {
            let table: Arc<dyn Table> = Arc::new(DummyTable::default());
            let mut expr = parse_exprs(ctx.clone(), table.clone(), default_expr)?;
            let mut expr = expr.remove(0);

            if expr.data_type() != &data_type {
                expr = Expr::Cast {
                    span: None,
                    is_try: data_type.is_nullable(),
                    expr: Box::new(expr),
                    dest_type: data_type,
                };
            }

            let dummy_block = DataBlock::new(vec![], 1);
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&dummy_block, &func_ctx, &BUILTIN_FUNCTIONS);
            let result = evaluator.run(&expr)?;

            match result {
                common_expression::Value::Scalar(s) => Ok(s),
                common_expression::Value::Column(c) if c.len() == 1 => {
                    let value = unsafe { c.index_unchecked(0) };
                    Ok(value.to_owned())
                }
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "Invalid default value for column: {}, must be constant, actual: {}",
                    field.name(),
                    result
                ))),
            }
        }
        None => Ok(Scalar::default_value(&data_type)),
    }
}
