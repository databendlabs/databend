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
use databend_common_ast::parser::parse_comma_separated_exprs;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::infer_table_schema;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use derive_visitor::DriveMut;
use parking_lot::RwLock;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::IdentifierNormalizer;
use crate::Metadata;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

pub fn bind_one_table(table_meta: Arc<dyn Table>) -> Result<(BindContext, MetadataRef)> {
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
        false,
        false,
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
    Ok((bind_context, metadata))
}

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Vec<Expr>> {
    let (mut bind_context, metadata) = bind_one_table(table_meta)?;
    let settings = Settings::create(Tenant::new_literal("dummy"));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let sql_dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    let tokens = tokenize_sql(sql)?;
    let ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    let exprs = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) = *type_checker.resolve(ast)?;
            let expr = scalar.as_expr()?.project_column_ref(|col| col.index);
            Ok(expr)
        })
        .collect::<Result<_>>()?;

    Ok(exprs)
}

pub fn parse_to_filters(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Filters> {
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
        let filter = exprs[0].clone();

        let inverted_filter = check_function(
            None,
            "not",
            &[],
            &[filter.as_expr(&BUILTIN_FUNCTIONS)],
            &BUILTIN_FUNCTIONS,
        )?;

        Ok(Filters {
            filter,
            inverted_filter: inverted_filter.as_remote_expr(),
        })
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
    let settings = Settings::create(Tenant::new_literal("dummy"));
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
    let sql_dialect = ctx.get_settings().get_sql_dialect()?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
    )?;

    let tokens = tokenize_sql(sql)?;
    let mut asts = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    if asts.len() != 1 {
        return Err(ErrorCode::BadDataValueType(format!(
            "Expected single expr, but got {}",
            asts.len()
        )));
    }
    let ast = asts.remove(0);
    let (scalar, _) = *type_checker.resolve(&ast)?;
    let expr = scalar.as_expr()?.project_column_ref(|col| col.index);
    Ok(expr)
}

pub fn parse_default_expr_string(
    ctx: &Arc<dyn TableContext>,
    default_expr: &str,
) -> Result<Box<(ScalarExpr, DataType)>> {
    let dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let tokens = tokenize_sql(default_expr)?;
    let ast = parse_expr(&tokens, dialect)?;
    parse_default_expr(ctx, &ast)
}

fn parse_default_expr(
    ctx: &Arc<dyn TableContext>,
    ast: &AExpr,
) -> Result<Box<(ScalarExpr, DataType)>> {
    let settings = Settings::create(Tenant::new_literal("dummy"));
    let mut bind_context = BindContext::new();
    let metadata = Metadata::default();

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
    )?;
    type_checker.resolve(ast)
}

pub fn parse_default_expr_to_string(
    ctx: Arc<dyn TableContext>,
    field: &TableField,
    ast: &AExpr,
    is_add_column: bool,
) -> Result<String> {
    let (mut scalar, data_type) = *parse_default_expr(&ctx, ast)?;
    let schema_data_type = DataType::from(field.data_type());
    if data_type != schema_data_type {
        scalar = wrap_cast(&scalar, &schema_data_type);
    }
    let expr = scalar.as_expr()?;

    // Added columns are not allowed to use expressions,
    // as the default values will be generated at each query.
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
    let settings = Settings::create(Tenant::new_literal("dummy"));
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
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
    )?;

    let (scalar, data_type) = *type_checker.resolve(ast)?;
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
    let mut normalizer = IdentifierNormalizer {
        ctx: &name_resolution_ctx,
    };
    ast.drive_mut(&mut normalizer);
    Ok(format!("{:#}", ast))
}

pub fn parse_lambda_expr(
    ctx: Arc<dyn TableContext>,
    columns: &[(String, DataType)],
    ast: &AExpr,
) -> Result<Box<(ScalarExpr, DataType)>> {
    let settings = Settings::create(Tenant::new_literal("dummy"));
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();

    bind_context.set_expr_context(ExprContext::InLambdaFunction);

    for (idx, column) in columns.iter().enumerate() {
        bind_context.add_column_binding(
            ColumnBindingBuilder::new(
                column.0.clone(),
                idx,
                Box::new(column.1.clone()),
                Visibility::Visible,
            )
            .build(),
        );

        let table_type = infer_schema_type(&column.1)?;
        metadata.add_base_table_column(column.0.to_string(), table_type, 0, None, None, None, None);
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
    )?;

    type_checker.resolve(ast)
}

pub fn parse_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    cluster_key_str: &str,
) -> Result<Vec<Expr>> {
    let (mut bind_context, metadata) = bind_one_table(table_meta)?;
    let settings = Settings::create(Tenant::new_literal("dummy"));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let sql_dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        true,
    )?;

    let tokens = tokenize_sql(cluster_key_str)?;
    let mut ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    // unwrap tuple.
    if ast_exprs.len() == 1 {
        if let AExpr::Tuple { exprs, .. } = &ast_exprs[0] {
            ast_exprs = exprs.clone();
        }
    } else {
        // Defensive check:
        // `ast_exprs` should always contain one element which can be one of the following:
        // 1. A tuple of composite cluster keys
        // 2. A single cluster key
        unreachable!("invalid cluster key ast expression, {:?}", ast_exprs);
    }

    let mut exprs = Vec::with_capacity(ast_exprs.len());
    for ast in ast_exprs {
        let (scalar, _) = *type_checker.resolve(&ast)?;
        let expr = scalar.as_expr()?.project_column_ref(|col| col.index);

        let inner_type = expr.data_type().remove_nullable();
        let mut should_wrapper = false;
        if inner_type == DataType::String {
            if let Expr::FunctionCall { function, .. } = &expr {
                should_wrapper = function.signature.name != "substr";
            } else {
                should_wrapper = true;
            }
        }

        // If the cluster key type is string, use substr to truncate the first 8 digits.
        let expr = if should_wrapper {
            check_function(
                None,
                "substr",
                &[],
                &[
                    expr,
                    Expr::Constant {
                        span: None,
                        scalar: Scalar::Number(1i64.into()),
                        data_type: DataType::Number(NumberDataType::Int64),
                    },
                    Expr::Constant {
                        span: None,
                        scalar: Scalar::Number(8u64.into()),
                        data_type: DataType::Number(NumberDataType::UInt64),
                    },
                ],
                &BUILTIN_FUNCTIONS,
            )?
        } else {
            expr
        };
        exprs.push(expr);
    }
    Ok(exprs)
}

pub fn analyze_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<(String, Vec<Expr>)> {
    let sql_dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let tokens = tokenize_sql(sql)?;
    let mut ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    // unwrap tuple.
    if ast_exprs.len() == 1 {
        if let AExpr::Tuple { exprs, .. } = &ast_exprs[0] {
            ast_exprs = exprs.clone();
        }
    }

    let (mut bind_context, metadata) = bind_one_table(table_meta)?;
    let settings = Settings::create(Tenant::new_literal("dummy"));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        true,
    )?;

    let mut exprs = Vec::with_capacity(ast_exprs.len());
    let mut cluster_keys = Vec::with_capacity(exprs.len());
    for ast in ast_exprs {
        let (scalar, _) = *type_checker.resolve(&ast)?;
        if scalar.used_columns().len() != 1 || !scalar.evaluable() {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Cluster by expression `{:#}` is invalid",
                ast
            )));
        }

        let expr = scalar.as_expr()?.project_column_ref(|col| col.index);
        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Cluster by expression `{:#}` is not deterministic",
                ast
            )));
        }

        let data_type = expr.data_type().remove_nullable();
        if !matches!(
            data_type,
            DataType::Number(_)
                | DataType::String
                | DataType::Timestamp
                | DataType::Date
                | DataType::Boolean
                | DataType::Decimal(_)
        ) {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Unsupported data type '{}' for cluster by expression `{:#}`",
                data_type, ast
            )));
        }

        exprs.push(expr);

        let mut cluster_by = ast.clone();
        let mut normalizer = IdentifierNormalizer {
            ctx: &name_resolution_ctx,
        };
        cluster_by.drive_mut(&mut normalizer);
        cluster_keys.push(format!("{:#}", &cluster_by));
    }

    let cluster_by_str = format!("({})", cluster_keys.join(", "));
    Ok((cluster_by_str, exprs))
}

#[derive(Default)]
struct DummyTable {
    info: TableInfo,
}
impl Table for DummyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_table_info(&self) -> &databend_common_meta_app::schema::TableInfo {
        &self.info
    }
}

pub fn field_default_value(ctx: Arc<dyn TableContext>, field: &TableField) -> Result<Scalar> {
    let data_type = field.data_type();
    let data_type = DataType::from(data_type);

    match field.default_expr() {
        Some(default_expr) => {
            let table: Arc<dyn Table> = Arc::new(DummyTable::default());
            let mut exprs = parse_exprs(ctx.clone(), table.clone(), default_expr)?;
            if exprs.len() != 1 {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Invalid default value for column: {}, expected single expr, but got: {}",
                    field.name(),
                    default_expr
                )));
            }
            let expr = exprs.remove(0);
            let expr = check_cast(
                None,
                false,
                expr,
                &field.data_type().into(),
                &BUILTIN_FUNCTIONS,
            )?;

            let dummy_block = DataBlock::new(vec![], 1);
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&dummy_block, &func_ctx, &BUILTIN_FUNCTIONS);
            let result = evaluator.run(&expr)?;

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
        None => Ok(Scalar::default_value(&data_type)),
    }
}
