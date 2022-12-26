use std::sync::Arc;

use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_base::base::tokio::runtime::Handle;
use common_base::base::tokio::task::block_in_place;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Expr;
use common_settings::Settings;
use parking_lot::RwLock;

use crate::executor::PhysicalScalar;
use crate::executor::PhysicalScalarBuilder;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::Scalar;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::Metadata;
use crate::MetadataRef;
use crate::Visibility;

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    sql: &str,
) -> Result<Vec<Expr>> {
    let sql_dialect = Dialect::MySQL;
    let tokens = tokenize_sql(sql)?;
    let backtrace = Backtrace::new();
    let exprs = parse_comma_separated_exprs(&tokens[1..tokens.len()], sql_dialect, &backtrace)?;

    let settings = Settings::default_settings("", GlobalConfig::instance())?;
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
    );

    let columns = metadata.read().columns_by_table_index(table_index);
    let table = metadata.read().table(table_index).clone();
    for column in columns.iter() {
        let column_binding = match column {
            ColumnEntry::BaseTableColumn {
                column_index,
                column_name,
                data_type,
                path_indices,
                ..
            } => ColumnBinding {
                database_name: Some("default".to_string()),
                table_name: Some(table.name().to_string()),
                column_name: column_name.clone(),
                index: *column_index,
                data_type: Box::new(data_type.into()),
                visibility: if path_indices.is_some() {
                    Visibility::InVisible
                } else {
                    Visibility::Visible
                },
            },

            _ => {
                return Err(ErrorCode::Internal("Invalid column entry"));
            }
        };

        bind_context.add_column_binding(column_binding);
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker = TypeChecker::new(
        &bind_context,
        ctx,
        &name_resolution_ctx,
        metadata.clone(),
        &[],
    );
    let mut expressions = Vec::with_capacity(exprs.len());

    for expr in exprs.iter() {
        let (scalar, _) =
            *block_in_place(|| Handle::current().block_on(type_checker.resolve(expr, None)))?;
        let mut builder = PhysicalScalarBuilder::new();
        let scalar = builder.build(&scalar)?;
        expressions.push(scalar.as_expr()?);
    }
    Ok(expressions)
}
