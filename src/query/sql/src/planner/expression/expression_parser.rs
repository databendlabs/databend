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
pub use databend_common_ast::parser::parse_cluster_key_exprs;
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
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionCall;
use databend_common_expression::Scalar;
use databend_common_expression::Symbol;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::infer_table_schema;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::table::HILBERT_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use derive_visitor::DriveMut;
use parking_lot::RwLock;

use crate::BaseTableColumn;
use crate::Binder;
use crate::ClusterKeyNormalizer;
use crate::ColumnBinding;
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

pub const HILBERT_CLUSTER_DIMENSIONS: usize = 2;
const CLUSTER_KEY_STRING_PREFIX_LEN: u64 = 8;

/// Parsed and bound cluster keys, encoded by their mutually exclusive layout.
#[derive(Clone, Debug)]
pub enum ClusterKeys {
    /// Ordinary lexicographic cluster keys.
    Linear(Vec<Expr<usize>>),
    /// Lexicographic keys containing one vector key at `vector_index`.
    Vector {
        keys: Vec<Expr<usize>>,
        vector_index: usize,
    },
    /// Dimensions used for Hilbert clustering and MBR statistics.
    Hilbert(Vec<Expr<usize>>),
}

impl ClusterKeys {
    pub fn into_keys(self) -> Vec<Expr<usize>> {
        match self {
            Self::Linear(keys) | Self::Vector { keys, .. } => keys,
            Self::Hilbert(dimensions) => dimensions,
        }
    }

    /// Return expressions persisted in `ClusterStatistics`.
    pub fn into_stats_keys(self) -> Vec<Expr<usize>> {
        match self {
            Self::Vector {
                mut keys,
                vector_index,
            } => {
                debug_assert!(vector_index < keys.len());
                keys.remove(vector_index);
                keys
            }
            Self::Linear(keys) => keys,
            Self::Hilbert(dimensions) => dimensions,
        }
    }

    pub fn is_linear(&self) -> bool {
        matches!(self, Self::Linear(_))
    }

    pub fn is_hilbert(&self) -> bool {
        matches!(self, Self::Hilbert(_))
    }
}

fn normalize_cluster_key_expr(expr: Expr<usize>) -> Result<Expr<usize>> {
    let is_substr = matches!(
        &expr,
        Expr::FunctionCall(FunctionCall { function, .. })
            if function.signature.name == "substr"
    );
    if expr.data_type().remove_nullable() != DataType::String || is_substr {
        return Ok(expr);
    }

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
                scalar: Scalar::Number(CLUSTER_KEY_STRING_PREFIX_LEN.into()),
                data_type: DataType::Number(NumberDataType::UInt64),
            }
            .into(),
        ],
        &BUILTIN_FUNCTIONS,
    )
}

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

    {
        let metadata = metadata.read();
        let table = metadata.table(table_index);
        for column in metadata.columns_by_table_index(table_index) {
            let column_binding = match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_index,
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
                        *column_index,
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
    }
    Ok((bind_context, metadata))
}

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Vec<Expr<ColumnBinding>>> {
    let sql_dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
    let tokens = tokenize_sql(sql)?;
    let ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
    parse_ast_exprs(ctx, table_meta, ast_exprs)
}

pub fn parse_exprs_to_field_index(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Vec<Expr<FieldIndex>>> {
    parse_exprs(ctx, table_meta, sql)?
        .into_iter()
        .map(|expr| expr.project_column_ref(|binding| Ok(binding.index.as_field_index())))
        .collect()
}

fn parse_ast_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    ast_exprs: Vec<AExpr>,
) -> Result<Vec<Expr<ColumnBinding>>> {
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
            let expr = scalar.as_expr()?;
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
    let exprs = parse_exprs(ctx, table_meta, sql)?
        .into_iter()
        .map(|expr| {
            Ok(expr
                .project_column_ref(|binding| {
                    Ok(schema
                        .field(binding.index.as_field_index())
                        .name()
                        .to_string())
                })?
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
) -> Result<Expr<ColumnBinding>> {
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();
    let table_schema = infer_table_schema(&schema)?;
    for (index, field) in schema.fields().iter().enumerate() {
        let table_field = table_schema.field(index);
        let column_index = metadata.add_base_table_column(
            table_field.name().clone(),
            table_field.data_type().clone(),
            0,
            None,
            index as ColumnId,
            None,
            None,
        );
        let column = ColumnBindingBuilder::new(
            field.name().clone(),
            column_index,
            Box::new(field.data_type().clone()),
            Visibility::Visible,
        )
        .build();
        bind_context.add_column_binding(column);
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
    let expr = scalar.as_expr()?;
    Ok(expr)
}

pub fn parse_computed_field_index_expr(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    sql: &str,
) -> Result<Expr<FieldIndex>> {
    parse_computed_expr(ctx, schema, sql)?
        .project_column_ref(|binding| Ok(binding.index.as_field_index()))
}

pub fn parse_computed_expr_to_string(
    ctx: Arc<dyn TableContext>,
    table_schema: TableSchemaRef,
    field: &TableField,
    ast: &AExpr,
) -> Result<String> {
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();
    for field in table_schema.fields().iter() {
        let column_index = metadata.add_base_table_column(
            field.name().clone(),
            field.data_type().clone(),
            0,
            None,
            field.column_id,
            None,
            None,
        );
        bind_context.add_column_binding(
            ColumnBindingBuilder::new(
                field.name().clone(),
                column_index,
                Box::new(field.data_type().into()),
                Visibility::Visible,
            )
            .build(),
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
    lambda_context.expr_context = ExprContext::InLambdaFunction;

    for (lambda_column, lambda_column_type) in lambda_columns.iter() {
        let column_index = lambda_context.next_column_index();
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
) -> Result<ClusterKeys> {
    let is_hilbert = table_meta
        .options()
        .get(OPT_KEY_CLUSTER_TYPE)
        .is_some_and(|value| value.eq_ignore_ascii_case(HILBERT_CLUSTER_TYPE));
    if is_hilbert && ast_exprs.len() != HILBERT_CLUSTER_DIMENSIONS {
        return Err(ErrorCode::InvalidClusterKeys(format!(
            "Hilbert clustering requires exactly {HILBERT_CLUSTER_DIMENSIONS} dimensions"
        )));
    }

    let mut vector_index = None;
    let keys = bind_key_exprs(ctx, table_meta, ast_exprs)?;
    for (index, key) in keys.iter().enumerate() {
        if !matches!(key.data_type().remove_nullable(), DataType::Vector(_)) {
            continue;
        }
        if is_hilbert {
            return Err(ErrorCode::InvalidClusterKeys(
                "Hilbert clustering does not support vector dimensions",
            ));
        }
        if vector_index.replace(index).is_some() {
            return Err(ErrorCode::InvalidClusterKeys(
                "Only one vector column is supported in cluster by",
            ));
        }
    }
    let keys = keys
        .into_iter()
        .map(normalize_cluster_key_expr)
        .collect::<Result<Vec<_>>>()?;

    if is_hilbert {
        Ok(ClusterKeys::Hilbert(keys))
    } else if let Some(vector_index) = vector_index {
        Ok(ClusterKeys::Vector { keys, vector_index })
    } else {
        Ok(ClusterKeys::Linear(keys))
    }
}

/// Bind persisted table-key ASTs to table-schema field offsets.
pub fn bind_key_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    ast_exprs: Vec<AExpr>,
) -> Result<Vec<Expr<usize>>> {
    let schema = table_meta.schema();
    parse_ast_exprs(ctx, table_meta, ast_exprs)?
        .into_iter()
        .map(|expr| expr.project_column_ref(|col| schema.index_of(&col.column_name)))
        .collect()
}

pub fn analyze_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<(String, Vec<Expr<Symbol>>)> {
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
    let mut cluster_keys = Vec::with_capacity(ast_exprs.len());
    let mut vector_cluster_key_num = 0;
    for ast in &ast_exprs {
        let (scalar, _) = *type_checker.resolve(ast)?;
        if scalar.used_columns().len() != 1 || !scalar.evaluable() {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Cluster by expression `{:#}` is invalid",
                ast
            )));
        }

        let expr = scalar.as_symbol_expr()?;
        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Cluster by expression `{:#}` is not deterministic",
                ast
            )));
        }

        let data_type = expr.data_type();
        let (is_valid_type, is_vector_type) = Binder::valid_cluster_key_type(data_type);
        if !is_valid_type {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Unsupported data type '{}' for cluster by expression `{:#}`",
                data_type, ast
            )));
        }
        if is_vector_type {
            vector_cluster_key_num += 1;
            if vector_cluster_key_num > 1 {
                return Err(ErrorCode::InvalidClusterKeys(
                    "Only one vector column is supported in cluster by",
                ));
            }
        }

        exprs.push(expr);

        let mut cluster_by = ast.clone();
        cluster_by.drive_mut(&mut normalizer);
        cluster_keys.push(format!("{:#}", &cluster_by));
    }

    let cluster_by_str = format!("({})", cluster_keys.join(", "));
    Ok((cluster_by_str, exprs))
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;

    use super::*;

    #[test]
    fn test_normalize_string_cluster_key_expr() -> Result<()> {
        let expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::String,
            display_name: "s".to_string(),
        });

        let normalized = normalize_cluster_key_expr(expr)?;
        let Expr::FunctionCall(call) = &normalized else {
            panic!("string cluster key should be wrapped with substr");
        };
        assert_eq!(call.function.signature.name, "substr");
        assert_eq!(call.args.len(), 3);
        assert!(matches!(call.args[0], Expr::ColumnRef(_)));

        let normalized = normalize_cluster_key_expr(normalized)?;
        let Expr::FunctionCall(call) = normalized else {
            panic!("normalized string cluster key should remain substr");
        };
        assert!(matches!(call.args[0], Expr::ColumnRef(_)));
        Ok(())
    }
}
