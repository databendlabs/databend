use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::VariantDataType;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_storages_basic::NullTable;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn nested_get_virtual_column_rewrite_skips_intermediate_paths() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings.clone());
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let metadata = Arc::new(RwLock::new(Metadata::default()));

    let mut bind_context = virtual_column_bind_context(metadata.clone())?;
    let mut type_checker = TypeChecker::try_create_with_adapter(
        &mut bind_context,
        adapter,
        &name_resolution_ctx,
        metadata.clone(),
        &[],
    )?;

    let (first, _) = *type_checker.resolve(&parse_test_expr("get(get(v, 'a'), 0)")?)?;
    let (second, _) = *type_checker.resolve(&parse_test_expr("get(get(v, 'b'), 'c')")?)?;
    let (third, third_type) =
        *type_checker.resolve(&parse_test_expr("get_string(get(v, 'b'), 'c')")?)?;
    drop(type_checker);

    assert_bound_column_index(&first, 2);
    assert_bound_column_index(&second, 3);
    assert_eq!(third_type, DataType::Nullable(Box::new(DataType::String)));

    let virtual_columns = bind_context
        .bound_virtual_columns
        .iter()
        .map(|(name, (_, column_index))| (name.key_name.as_str(), column_index.as_usize()))
        .collect::<Vec<_>>();
    assert_eq!(virtual_columns, vec![("v['a'][0]", 2), ("v['b']['c']", 3)]);

    let metadata = metadata.read();
    assert_eq!(metadata.columns().len(), 4);
    assert_virtual_column(metadata.column(Symbol::new(2)), "v['a'][0]");
    assert_virtual_column(metadata.column(Symbol::new(3)), "v['b']['c']");

    let case = SqlTestCase {
        name: "nested_get_virtual_column_rewrite_skips_intermediate_paths",
        description: "Nested get/get_string calls should resolve a final virtual column without binding intermediate paths.",
        setup_sqls: &[],
        sql: "get(get(v, 'a'), 0); get(get(v, 'b'), 'c'); get_string(get(v, 'b'), 'c')",
    };
    let mut file = open_golden_file("semantic/type_check", "virtual_columns.txt")?;
    write_case_header(&mut file, &case)?;
    write_case_outcome_body(
        &mut file,
        &SqlTestOutcome::Plan(format!(
            "first_scalar: {}\nfirst_type: {}\nsecond_scalar: {}\nsecond_type: {}\nthird_scalar: {}\nthird_type: {}\nbound_virtual_columns:\n{}\nmetadata_columns:\n{}",
            format_scalar(&first),
            first.data_type()?,
            format_scalar(&second),
            second.data_type()?,
            format_scalar(&third),
            third_type,
            format_virtual_columns(&bind_context),
            format_metadata_columns(metadata.columns()),
        )),
    )?;

    Ok(())
}

fn virtual_column_bind_context(metadata: Arc<RwLock<Metadata>>) -> Result<BindContext> {
    let table = NullTable::try_create(TableInfo {
        desc: "'default'.'t2'".into(),
        name: "t2".into(),
        ident: Default::default(),
        meta: TableMeta {
            schema: TableSchemaRefExt::create(vec![
                TableField::new_from_column_id(
                    "a",
                    TableDataType::Number(NumberDataType::Int64),
                    0,
                ),
                TableField::new_from_column_id("v", TableDataType::Variant, 1),
            ]),
            engine: "Null".to_string(),
            virtual_schema: Some(VirtualDataSchema {
                fields: vec![
                    VirtualDataField {
                        name: "v['a'][0]".to_string(),
                        data_types: vec![VariantDataType::Jsonb],
                        source_column_id: 1,
                        column_id: 100,
                    },
                    VirtualDataField {
                        name: "v['b']['c']".to_string(),
                        data_types: vec![VariantDataType::Jsonb],
                        source_column_id: 1,
                        column_id: 101,
                    },
                ],
                next_column_id: 102,
                ..VirtualDataSchema::empty()
            }),
            ..Default::default()
        },
        ..Default::default()
    })?;

    let table_index = metadata.write().add_table(
        "default".to_string(),
        "default".to_string(),
        table.into(),
        None,
        None,
        false,
        false,
        false,
        None,
    );

    let mut bind_context = BindContext::new();
    bind_context.allow_virtual_column = true;
    for column in metadata.read().columns_by_table_index(table_index) {
        let ColumnEntry::BaseTableColumn(BaseTableColumn {
            column_name,
            column_index,
            data_type,
            column_position,
            ..
        }) = column
        else {
            continue;
        };
        bind_context.add_column_binding(
            ColumnBindingBuilder::new(
                column_name.clone(),
                column_index,
                Box::new(DataType::from(&data_type)),
                Visibility::Visible,
            )
            .table_name(Some("t2".to_string()))
            .database_name(Some("default".to_string()))
            .table_index(Some(table_index))
            .column_position(column_position)
            .build(),
        );
    }
    Ok(bind_context)
}

fn parse_test_expr(sql: &str) -> Result<databend_common_ast::ast::Expr> {
    let tokens = tokenize_sql(sql)?;
    Ok(parse_expr(
        &tokens,
        databend_common_ast::parser::Dialect::PostgreSQL,
    )?)
}

fn assert_bound_column_index(scalar: &ScalarExpr, expected: usize) {
    let ScalarExpr::BoundColumnRef(column) = scalar else {
        panic!("expected bound column ref, got {scalar:?}");
    };
    assert_eq!(column.column.index.as_usize(), expected);
}

fn assert_virtual_column(column: &ColumnEntry, expected_name: &str) {
    let ColumnEntry::VirtualColumn(column) = column else {
        panic!("expected virtual column, got {column:?}");
    };
    assert_eq!(column.column_name, expected_name);
}

fn format_virtual_columns(bind_context: &BindContext) -> String {
    bind_context
        .bound_virtual_columns
        .iter()
        .map(|(name, (_, column_index))| {
            format!("- {} -> #{}", name.key_name, column_index.as_usize())
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_metadata_columns(columns: &[ColumnEntry]) -> String {
    columns
        .iter()
        .map(|column| match column {
            ColumnEntry::BaseTableColumn(column) => {
                format!(
                    "- #{} base {}",
                    column.column_index.as_usize(),
                    column.column_name
                )
            }
            ColumnEntry::VirtualColumn(column) => {
                format!(
                    "- #{} virtual {}",
                    column.column_index.as_usize(),
                    column.column_name
                )
            }
            other => format!("- other {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
