use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::VIRTUAL_COLUMN_ID_START;
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
async fn test_type_check_variant_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "array_index_access_binds",
            description: "Array index access should preserve the existing get-function rewrite and nullable result type.",
            setup_sqls: &[],
            sql: "[10, 20, 30][1]",
        },
        SqlTestCase {
            name: "array_expression_index_access_binds",
            description: "Non-literal array indices should bind through get with normal type checking.",
            setup_sqls: &[],
            sql: "array('a', 'b', 'c')[delta % 2]",
        },
        SqlTestCase {
            name: "array_dynamic_index_before_literal_path_binds",
            description: "Literal access after a dynamic index should preserve the remaining path.",
            setup_sqls: &[],
            sql: "[[10, 20], [30, 40]][delta][2]",
        },
        SqlTestCase {
            name: "array_dynamic_index_after_literal_path_binds",
            description: "A dynamic index should preserve preceding literal access.",
            setup_sqls: &[],
            sql: "[[10, 20], [30, 40]][1][delta]",
        },
        SqlTestCase {
            name: "map_expression_key_access_binds",
            description: "Computed map keys should use the existing get function.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}[concat('k', '1')]",
        },
        SqlTestCase {
            name: "map_key_access_binds",
            description: "Map key access should preserve the existing get-function rewrite.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}['k1']",
        },
        SqlTestCase {
            name: "variant_colon_access_binds",
            description: "Variant colon access should preserve the get_by_keypath rewrite.",
            setup_sqls: &[],
            sql: "to_variant({'k1': 1}):k1",
        },
        SqlTestCase {
            name: "variant_get_with_quoted_unicode_key_stays_get",
            description: "A plain get call should not be lowered into a keypath expression before virtual-column resolution.",
            setup_sqls: &[],
            sql: "get(to_variant({'测试\"💎': 'a'}), '测试\"💎')",
        },
        SqlTestCase {
            name: "map_accessor_requires_literal_path",
            description: "Map and variant accessors should reject unsupported bracket expressions during lowering.",
            setup_sqls: &[],
            sql: "to_variant({'k1': 1})[true]",
        },
    ];

    run_type_check_cases("variant.txt", &cases).await
}

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
    assert_eq!(virtual_columns, vec![
        ("v['a'][0]", 2),
        ("v['b']['c']", 3),
        ("v['b']['c']", 4)
    ]);

    let metadata = metadata.read();
    assert_eq!(metadata.columns().len(), 5);
    assert_virtual_column(metadata.column(Symbol::new(2)), "v['a'][0]");
    assert_virtual_column(metadata.column(Symbol::new(3)), "v['b']['c']");

    let case = SqlTestCase {
        name: "nested_get_virtual_column_rewrite_skips_intermediate_paths",
        description: "Nested get/get_string calls should resolve a final virtual column without binding intermediate paths.",
        setup_sqls: &[],
        sql: "get(get(v, 'a'), 0); get(get(v, 'b'), 'c'); get_string(get(v, 'b'), 'c')",
    };
    let mut file = open_golden_file("semantic/type_check", "variant_virtual_columns.txt")?;
    write_case_header(&mut file, &case)?;
    write_case_outcome_body(
        &mut file,
        &SqlTestOutcome::Plan(format!(
            "first_scalar: {}\nfirst_type: {}\nsecond_scalar: {}\nsecond_type: {}\nthird_scalar: {}\nthird_type: {}\nbound_virtual_columns:\n{}\nmetadata_columns:\n{}",
            format_scalar(&first),
            first.data_type(),
            format_scalar(&second),
            second.data_type(),
            format_scalar(&third),
            third_type,
            format_virtual_columns(&bind_context),
            format_metadata_columns(metadata.columns()),
        )),
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn variant_cast_pushdown_and_non_column_fallback() -> Result<()> {
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

    let array_type = DataType::Nullable(Box::new(DataType::Array(Box::new(DataType::Nullable(
        Box::new(DataType::Number(NumberDataType::Int32)),
    )))));
    let nullable_variant = DataType::Nullable(Box::new(DataType::Variant));
    let nullable_int64 = DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)));
    let cases = [
        ("v['a']::ARRAY(INT32)", true, false, Some(2), array_type),
        (
            "v['b']['c']::Int64",
            false,
            false,
            Some(3),
            nullable_int64.clone(),
        ),
        (
            "TRY_CAST(v['b']['c'] AS Int64)",
            false,
            false,
            Some(4),
            nullable_int64.clone(),
        ),
        (
            "get_string(get(v, 'b'), 'c')::Int64",
            true,
            false,
            Some(5),
            nullable_int64.clone(),
        ),
        ("get(v, 'e')", false, false, Some(6), nullable_variant),
        (
            "parse_json('{\"k\":1}')['k']::Int64",
            true,
            false,
            None,
            nullable_int64,
        ),
    ];

    for (sql, has_cast, is_try, virtual_column_index, expected_type) in cases {
        let (scalar, data_type) = *type_checker.resolve(&parse_test_expr(sql)?)?;
        let scalar = if has_cast {
            let ScalarExpr::CastExpr(cast) = scalar else {
                panic!("expected outer cast for {sql}, got {scalar:?}");
            };
            assert_eq!(cast.target_type.as_ref(), &data_type);
            assert_eq!(cast.is_try, is_try, "unexpected cast mode for {sql}");
            *cast.argument
        } else {
            assert!(
                !matches!(&scalar, ScalarExpr::CastExpr(_)),
                "unexpected outer cast for {sql}"
            );
            scalar
        };

        if let Some(column_index) = virtual_column_index {
            assert_bound_column_index(&scalar, column_index);
        } else {
            // A computed Variant base cannot bind a virtual column. Its already-resolved
            // scalar is reused to build get_by_keypath below the requested cast.
            let ScalarExpr::FunctionCall(access) = scalar else {
                panic!("expected get_by_keypath fallback for {sql}, got {scalar:?}");
            };
            assert_eq!(access.func_name, "get_by_keypath");
            assert_eq!(access.arguments.len(), 2);
            assert_eq!(
                access.arguments[0].data_type().remove_nullable(),
                DataType::Variant
            );
            assert!(!matches!(
                &access.arguments[0],
                ScalarExpr::BoundColumnRef(_)
            ));
        }

        assert_eq!(data_type, expected_type, "unexpected type for {sql}");
    }
    drop(type_checker);

    let metadata = metadata.read();
    let virtual_columns = metadata
        .columns()
        .iter()
        .filter_map(|column| match column {
            ColumnEntry::VirtualColumn(column) => Some((
                column.column_index.as_usize(),
                column.source_column_id,
                column.query_column_id,
                column.column_name.as_str(),
                column.data_type.clone(),
                column.is_try,
                column.key_paths.to_canonical_path(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(virtual_columns, vec![
        (
            2,
            1,
            VIRTUAL_COLUMN_ID_START,
            "v['a']",
            TableDataType::Nullable(Box::new(TableDataType::Variant)),
            true,
            "a".to_string(),
        ),
        (
            3,
            1,
            VIRTUAL_COLUMN_ID_START + 1,
            "v['b']['c']::Int64",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
            false,
            "b.c".to_string(),
        ),
        (
            4,
            1,
            VIRTUAL_COLUMN_ID_START + 2,
            "try_cast(v['b']['c'] AS Int64)",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
            true,
            "b.c".to_string(),
        ),
        (
            5,
            1,
            VIRTUAL_COLUMN_ID_START + 3,
            "v['b']['c']::String",
            TableDataType::Nullable(Box::new(TableDataType::String)),
            false,
            "b.c".to_string(),
        ),
        (
            6,
            1,
            VIRTUAL_COLUMN_ID_START + 4,
            "v['e']",
            TableDataType::Nullable(Box::new(TableDataType::Variant)),
            true,
            "e".to_string(),
        ),
    ]);

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
            options: [("enable_virtual_column".to_string(), "true".to_string())].into(),
            virtual_schema: Some(VirtualDataSchema {
                fields: vec![
                    VirtualDataField {
                        name: "v.a[0]".to_string(),
                        data_types: vec![VariantDataType::Jsonb],
                        source_column_id: 1,
                        column_id: 100,
                    },
                    VirtualDataField {
                        name: "v.b.c".to_string(),
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
                *column_index,
                Box::new(DataType::from(data_type)),
                Visibility::Visible,
            )
            .table_name(Some("t2".to_string()))
            .database_name(Some("default".to_string()))
            .table_index(Some(table_index))
            .column_position(*column_position)
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
