use std::sync::Arc;

use common_ast::ast::Expr as AExpr;
use common_ast::parser::parse_expr;
use common_ast::parser::parser_values_with_placeholder;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::table_context::StageAttachment;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::Value;
use common_meta_app::principal::StageInfo;
use common_pipeline_transforms::processors::transforms::Transform;
use common_sql::binder::wrap_cast;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::plans::FunctionCall;
use common_sql::BindContext;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_sql::ScalarBinder;
use common_sql::ScalarExpr;
use common_storage::StageFileInfo;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use parking_lot::RwLock;
use tracing::error;

use crate::sessions::QueryContext;

#[async_backtrace::framed]
pub async fn prepared_values(
    ctx: &Arc<QueryContext>,
    source_schema: &DataSchemaRef,
    attachment: &Arc<StageAttachment>,
) -> Result<(DataSchemaRef, Vec<Scalar>)> {
    let settings = ctx.get_settings();
    let sql_dialect = settings.get_sql_dialect()?;
    let tokens = tokenize_sql(attachment.values_str.as_str())?;
    let expr_or_placeholders = parser_values_with_placeholder(&tokens, sql_dialect)?;

    if source_schema.num_fields() != expr_or_placeholders.len() {
        return Err(ErrorCode::SemanticError(format!(
            "need {} fields in values, got only {}",
            source_schema.num_fields(),
            expr_or_placeholders.len()
        )));
    }

    let mut attachment_fields = vec![];
    let mut const_fields = vec![];
    let mut exprs = vec![];
    for (i, eo) in expr_or_placeholders.into_iter().enumerate() {
        match eo {
            Some(e) => {
                exprs.push(e);
                const_fields.push(source_schema.fields()[i].clone());
            }
            None => attachment_fields.push(source_schema.fields()[i].clone()),
        }
    }
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let const_schema = Arc::new(DataSchema::new(const_fields));
    let const_values = exprs_to_scalar(
        exprs,
        &const_schema,
        ctx.clone(),
        &name_resolution_ctx,
        &mut bind_context,
        metadata,
    )
    .await?;
    Ok((Arc::new(DataSchema::new(attachment_fields)), const_values))
}

#[async_backtrace::framed]
pub async fn try_purge_files(
    ctx: Arc<QueryContext>,
    stage_info: &StageInfo,
    stage_files: &[StageFileInfo],
) {
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let op = StageTable::get_op(stage_info);
    match op {
        Ok(op) => {
            let file_op = Files::create(table_ctx, op);
            let files = stage_files
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();
            if let Err(e) = file_op.remove_file_in_batch(&files).await {
                error!("Failed to delete file: {:?}, error: {}", files, e);
            }
        }
        Err(e) => {
            error!("Failed to get stage table op, error: {}", e);
        }
    }
}

pub async fn exprs_to_scalar(
    exprs: Vec<AExpr>,
    schema: &DataSchemaRef,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &NameResolutionContext,
    bind_context: &mut BindContext,
    metadata: MetadataRef,
) -> Result<Vec<Scalar>> {
    let schema_fields_len = schema.fields().len();
    if exprs.len() != schema_fields_len {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Table columns count is not match, expect {schema_fields_len}, input: {}, expr: {:?}",
            exprs.len(),
            exprs
        )));
    }
    let mut scalar_binder = ScalarBinder::new(
        bind_context,
        ctx.clone(),
        name_resolution_ctx,
        metadata.clone(),
        &[],
    );

    let mut map_exprs = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
        if let AExpr::ColumnRef { column, .. } = expr {
            if column.name.eq_ignore_ascii_case("default") {
                let field = schema.field(i);
                fill_default_value(&mut scalar_binder, &mut map_exprs, field, schema).await?;
                continue;
            }
        }

        let (mut scalar, data_type) = scalar_binder.bind(expr).await?;
        let field_data_type = schema.field(i).data_type();
        scalar = if field_data_type.remove_nullable() == DataType::Variant {
            match data_type.remove_nullable() {
                DataType::Boolean
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::Timestamp
                | DataType::Date
                | DataType::Bitmap
                | DataType::Variant => wrap_cast(&scalar, field_data_type),
                DataType::String => {
                    // parse string to JSON value
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "parse_json".to_string(),
                        params: vec![],
                        arguments: vec![scalar],
                    })
                }
                _ => {
                    if data_type == DataType::Null && field_data_type.is_nullable() {
                        scalar
                    } else {
                        return Err(ErrorCode::BadBytes(format!(
                            "unable to cast type `{}` to type `{}`",
                            data_type, field_data_type
                        )));
                    }
                }
            }
        } else {
            wrap_cast(&scalar, field_data_type)
        };
        let expr = scalar
            .as_expr()?
            .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
        map_exprs.push(expr);
    }

    let mut operators = Vec::with_capacity(schema_fields_len);
    operators.push(BlockOperator::Map { exprs: map_exprs });

    let one_row_chunk = DataBlock::new(
        vec![BlockEntry {
            data_type: DataType::Number(NumberDataType::UInt8),
            value: Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
        }],
        1,
    );
    let func_ctx = ctx.get_function_context()?;
    let mut expression_transform = CompoundBlockOperator {
        operators,
        ctx: func_ctx,
    };
    let res = expression_transform.transform(one_row_chunk)?;
    let scalars: Vec<Scalar> = res
        .columns()
        .iter()
        .skip(1)
        .map(|col| unsafe { col.value.as_ref().index_unchecked(0).to_owned() })
        .collect();
    Ok(scalars)
}

pub async fn fill_default_value(
    binder: &mut ScalarBinder<'_>,
    map_exprs: &mut Vec<Expr>,
    field: &DataField,
    schema: &DataSchema,
) -> Result<()> {
    if let Some(default_expr) = field.default_expr() {
        let tokens = tokenize_sql(default_expr)?;
        let ast = parse_expr(&tokens, Dialect::PostgreSQL)?;
        let (mut scalar, _) = binder.bind(&ast).await?;
        scalar = wrap_cast(&scalar, field.data_type());

        let expr = scalar
            .as_expr()?
            .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
        map_exprs.push(expr);
    } else {
        // If field data type is nullable, then we'll fill it with null.
        if field.data_type().is_nullable() {
            let expr = Expr::Constant {
                span: None,
                scalar: Scalar::Null,
                data_type: field.data_type().clone(),
            };
            map_exprs.push(expr);
        } else {
            let data_type = field.data_type().clone();
            let default_value = Scalar::default_value(&data_type);
            let expr = Expr::Constant {
                span: None,
                scalar: default_value,
                data_type,
            };
            map_exprs.push(expr);
        }
    }
    Ok(())
}
