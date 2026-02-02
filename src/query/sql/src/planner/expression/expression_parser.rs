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
use databend_common_ast::parser::parse_cluster_key_exprs;
use databend_common_ast::parser::parse_comma_separated_exprs;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Constant;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::FunctionCall;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::infer_table_schema;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use derive_visitor::DriveMut;
use parking_lot::RwLock;

use crate::BaseTableColumn;
use crate::Binder;
use crate::ClusterKeyNormalizer;
use crate::ColumnEntry;
use crate::IdentifierNormalizer;
use crate::Metadata;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;

pub fn bind_table(table_meta: Arc<dyn Table>) -> Result<(BindContext, MetadataRef)> {
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
        None,
        false,
        false,
        false,
        None,
    );

    let columns = metadata.read().columns_by_table_index(table_index);
    let table = metadata.read().table(table_index).clone();
    for (index, column) in columns.iter().enumerate() {
        let column_binding = match column {
            ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_name,
                data_type,
                path_indices,
                virtual_expr,
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
                .virtual_expr(virtual_expr.clone())
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
    let sql_dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let tokens = tokenize_sql(sql)?;
    let ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    parse_ast_exprs(ctx, table_meta, ast_exprs)
}

fn parse_ast_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    ast_exprs: Vec<AExpr>,
) -> Result<Vec<Expr>> {
    let (mut bind_context, metadata) = bind_table(table_meta)?;
    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;

    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    let exprs = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) = *type_checker.resolve(ast)?;
            let expr = scalar.as_expr()?.project_column_ref(|col| Ok(col.index))?;
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
            Ok(expr
                .project_column_ref(|index| Ok(schema.field(*index).name().to_string()))?
                .as_remote_expr())
        })
        .collect::<Result<Vec<_>>>()?;

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
            index as ColumnId,
            None,
            None,
        );
    }

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        Arc::new(RwLock::new(metadata)),
        &[],
        false,
    )?;

    let tokens = tokenize_sql(sql)?;
    let sql_dialect = settings.get_sql_dialect()?;
    let mut asts = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    if asts.len() != 1 {
        return Err(ErrorCode::BadDataValueType(format!(
            "Expected single expr, but got {}",
            asts.len()
        )));
    }
    let ast = asts.remove(0);
    let (scalar, _) = *type_checker.resolve(&ast)?;
    let expr = scalar.as_expr()?.project_column_ref(|col| Ok(col.index))?;
    Ok(expr)
}

pub fn parse_computed_expr_to_string(
    ctx: Arc<dyn TableContext>,
    table_schema: TableSchemaRef,
    field: &TableField,
    ast: &AExpr,
) -> Result<String> {
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
            field.column_id,
            None,
            None,
        );
    }

    let settings = ctx.get_settings();
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
    if !scalar.evaluable() {
        return Err(ErrorCode::SemanticError(format!(
            "computed column expression `{:#}` is invalid",
            ast
        )));
    }
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
    let mut normalizer = IdentifierNormalizer::new(&name_resolution_ctx);
    ast.drive_mut(&mut normalizer);
    Ok(format!("{:#}", ast))
}

pub fn parse_lambda_expr(
    ctx: Arc<dyn TableContext>,
    lambda_context: &mut BindContext,
    lambda_columns: &[(String, DataType)],
    ast: &AExpr,
    parent_metadata: Option<Arc<RwLock<Metadata>>>,
) -> Result<Box<(ScalarExpr, DataType)>> {
    // Use parent metadata if provided (for masking policies on outer columns)
    // Otherwise create empty metadata (for better performance in community edition)
    let metadata = parent_metadata.unwrap_or_else(|| Arc::new(RwLock::new(Metadata::default())));
    lambda_context.set_expr_context(ExprContext::InLambdaFunction);

    // The column index may not be consecutive, and the length of columns
    // cannot be used to calculate the column index of the lambda argument.
    // We need to start from the current largest column index.
    let mut column_index = lambda_context
        .all_column_bindings()
        .iter()
        .map(|c| c.index)
        .max()
        .unwrap_or_default();
    for (lambda_column, lambda_column_type) in lambda_columns.iter() {
        column_index += 1;
        lambda_context.add_column_binding(
            ColumnBindingBuilder::new(
                lambda_column.clone(),
                column_index,
                Box::new(lambda_column_type.clone()),
                Visibility::Visible,
            )
            .build(),
        );
    }

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        lambda_context,
        ctx.clone(),
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    type_checker.resolve(ast)
}

pub fn parse_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    ast_exprs: Vec<AExpr>,
) -> Result<Vec<Expr>> {
    let schema = table_meta.schema();
    let (mut bind_context, metadata) = bind_table(table_meta)?;
    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    let exprs: Vec<Expr> = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) = *type_checker.resolve(ast)?;
            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| schema.index_of(&col.column_name))?;
            Ok(expr)
        })
        .collect::<Result<_>>()?;

    let mut res = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let inner_type = expr.data_type().remove_nullable();
        let mut should_wrapper = false;
        if inner_type == DataType::String {
            if let Expr::FunctionCall(FunctionCall { function, .. }) = &expr {
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
                    Constant {
                        span: None,
                        scalar: Scalar::Number(1i64.into()),
                        data_type: DataType::Number(NumberDataType::Int64),
                    }
                    .into(),
                    Constant {
                        span: None,
                        scalar: Scalar::Number(8u64.into()),
                        data_type: DataType::Number(NumberDataType::UInt64),
                    }
                    .into(),
                ],
                &BUILTIN_FUNCTIONS,
            )?
        } else {
            expr
        };
        res.push(expr);
    }
    Ok(res)
}

pub fn analyze_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<(String, Vec<Expr>)> {
    let ast_exprs = parse_cluster_key_exprs(sql)?;
    let (mut bind_context, metadata) = bind_table(table_meta)?;
    let name_resolution_ctx = NameResolutionContext::try_from(ctx.get_settings().as_ref())?;
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        metadata,
        &[],
        true,
    )?;

    let settings = ctx.get_settings();
    let mut normalizer = ClusterKeyNormalizer {
        force_quoted_ident: false,
        unquoted_ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
        quoted_ident_case_sensitive: settings.get_quoted_ident_case_sensitive()?,
        sql_dialect: settings.get_sql_dialect()?,
    };
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

        let expr = scalar.as_expr()?.project_column_ref(|col| Ok(col.index))?;
        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Cluster by expression `{:#}` is not deterministic",
                ast
            )));
        }

        let data_type = expr.data_type();
        if !Binder::valid_cluster_key_type(data_type) {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Unsupported data type '{}' for cluster by expression `{:#}`",
                data_type, ast
            )));
        }

        exprs.push(expr);

        let mut cluster_by = ast.clone();
        cluster_by.drive_mut(&mut normalizer);
        cluster_keys.push(format!("{:#}", &cluster_by));
    }

    let cluster_by_str = format!("({})", cluster_keys.join(", "));
    Ok((cluster_by_str, exprs))
}
