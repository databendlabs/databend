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

use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn variant_paths_through_conditionals_bind_untyped_virtual_columns() -> Result<()> {
    init_testing_globals();
    for sql in [
        "(CASE WHEN a > 0 THEN v ELSE v END)['b']['c']",
        "get(if(a > 0, v, v), 'a')",
        "get_string(if(a > 0, v, v), 'a')",
        "(CASE WHEN a > 0 THEN v ELSE object_delete(v, 'secret') END)['a']::INT64",
        "TRY_CAST(if(a > 0, v, v)['a'] AS INT64)",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings.clone());
        let names = NameResolutionContext::try_from(settings.as_ref())?;
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let mut context = super::variant::virtual_column_bind_context(metadata.clone())?;
        let mut checker = TypeChecker::try_create_with_adapter(
            &mut context,
            adapter,
            &names,
            metadata.clone(),
            &[],
        )?;
        let tokens = tokenize_sql(sql)?;
        let expr = parse_expr(&tokens, databend_common_ast::parser::Dialect::PostgreSQL)?;
        let (scalar, _) = *checker.resolve(&expr)?;
        drop(checker);
        assert!(!context.bound_virtual_columns.is_empty(), "{sql}");
        for name in context.bound_virtual_columns.keys() {
            // A cast must not be pushed across a conditional, even if all leaves bind.
            assert!(name.target_cast.is_none(), "{sql}: {name:?}");
        }
        assert!(format_scalar(&scalar).contains("if"), "{sql}: {scalar:?}");
        if sql.contains("object_delete") {
            assert!(format_scalar(&scalar).contains("object_delete"));
        } else {
            // All result leaves use paths; the original JSON column is unnecessary.
            assert!(!scalar.used_columns().contains(&Symbol::new(1)), "{sql}");
        }
    }
    let mut context = BindContext::new();
    add_test_column(&mut context, 0, "v", DataType::Variant);
    for sql in [
        "object_delete(object_construct('a', v), 'missing')['a']",
        "object_delete(object_construct_keep_null('a', v), 'missing')['a']",
        "object_delete(try_object_construct('a', v), 'missing')['a']",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let (scalar, _) =
            resolve_type_check_sql(sql, TestTypeCheckAdapter::new(settings), &mut context)?;
        assert!(
            !format_scalar(&scalar).contains("object_delete"),
            "{sql}: {scalar:?}"
        );
    }
    for sql in [
        "object_delete(v, 'missing')['a']",
        // Returning a missing key must not skip a constructor's fallible arguments.
        "object_delete(object_construct('secret', parse_json(v::STRING)), 'secret')['secret']",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let (scalar, _) =
            resolve_type_check_sql(sql, TestTypeCheckAdapter::new(settings), &mut context)?;
        assert!(
            format_scalar(&scalar).contains("object_delete"),
            "{sql}: {scalar:?}"
        );
    }
    Ok(())
}

// The reference deliberately uses a non-literal key AST. It folds to the same key during
// normal type checking, but cannot enter StaticVariantAccess or the new path rewrite.
fn evaluate_path(
    base: &str,
    path: &str,
    rewrite: bool,
    json: Scalar,
    flag: Scalar,
) -> Result<(DataType, Vec<Scalar>)> {
    let block = DataBlock::new(
        vec![
            BlockEntry::new_const_column(DataType::Variant.wrap_nullable(), json, 3),
            BlockEntry::new_const_column(DataType::Boolean.wrap_nullable(), flag, 3),
        ],
        3,
    );
    evaluate_block(base, path, rewrite, &block)
}

fn evaluate_block(
    base: &str,
    path: &str,
    rewrite: bool,
    block: &DataBlock,
) -> Result<(DataType, Vec<Scalar>)> {
    // GET_BY_KEYPATH uses PostgreSQL-style `{a,b}` paths, not JSON/colon paths.
    let path = format!("{{{}}}", path.replace(':', ",")).replace('\'', "''");
    let key = if rewrite {
        format!("'{path}'")
    } else {
        format!("concat('{path}', '')")
    };
    let sql = format!("get_by_keypath({base}, {key})");
    let settings = Settings::create(Tenant::new_literal("default"));
    let mut context = BindContext::new();
    add_test_column(&mut context, 0, "v", DataType::Variant);
    add_test_column(&mut context, 1, "flag", DataType::Boolean);
    let (scalar, data_type) =
        resolve_type_check_sql(&sql, TestTypeCheckAdapter::new(settings), &mut context)?;
    let expr = scalar
        .as_expr()?
        .project_column_ref(|col| Ok(col.index.as_usize()))?;
    let func_ctx = FunctionContext::default();
    let result: Value<AnyType> = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS).run(&expr)?;
    let rows = (0..block.num_rows())
        .map(|row| result.index(row).unwrap().to_owned())
        .collect();
    Ok((data_type, rows))
}

fn compare_paths(base: &str, path: &str, json: Scalar, flag: Scalar) {
    let original = evaluate_path(base, path, false, json.clone(), flag.clone());
    let rewritten = evaluate_path(base, path, true, json, flag);
    match (original, rewritten) {
        (Ok(original), Ok(rewritten)) => assert_eq!(original, rewritten, "{base}, {path}"),
        (Err(original), Err(rewritten)) => {
            assert_eq!(original.code(), rewritten.code(), "{base}, {path}");
            // Runtime diagnostics include the evaluated expression, which changes with
            // rewriting. Check the function's error payload separately from that context.
            assert_eq!(
                original
                    .message()
                    .split(" while evaluating function")
                    .next(),
                rewritten
                    .message()
                    .split(" while evaluating function")
                    .next(),
                "{base}, {path}",
            );
        }
        (original, rewritten) => panic!("{base}, {path}: {original:?} != {rewritten:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn variant_path_rewrites_preserve_values_and_errors() {
    init_testing_globals();
    let bases = [
        "if(flag, v, object_delete(v, 'secret'))",
        "CASE WHEN flag THEN v WHEN flag IS NULL THEN NULL ELSE object_delete(v, 'secret') END",
        "object_delete(v, 'missing', 'secret', NULL)",
        "object_delete(object_construct('a', v, 'secret', 1), 'missing', 'secret', NULL)",
        "object_delete(object_construct_keep_null('a', v), 'secret')",
        "object_delete(try_object_construct('a', v), 'secret')",
        "object_delete(object_construct('secret', parse_json(v::STRING)), 'secret')",
    ];
    for input in [
        None,
        Some("null"),
        Some("42"),
        Some("\"text\""),
        Some("[]"),
        Some("{}"),
        Some("{\"a\":null}"),
        Some("{\"a\":{\"b\":1},\"secret\":2}"),
    ] {
        let json = input
            .map(|s| Scalar::Variant(jsonb::parse_owned_jsonb(s.as_bytes()).unwrap().to_vec()))
            .unwrap_or(Scalar::Null);
        for base in bases {
            for path in ["a", "secret", "missing", "a:b"] {
                for flag in [Scalar::Boolean(true), Scalar::Boolean(false), Scalar::Null] {
                    compare_paths(base, path, json.clone(), flag);
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn variant_path_rewrites_preserve_mixed_row_short_circuit() -> Result<()> {
    init_testing_globals();
    let mut json = ColumnBuilder::with_capacity(&DataType::Variant.wrap_nullable(), 3);
    let mut flag = ColumnBuilder::with_capacity(&DataType::Boolean.wrap_nullable(), 3);
    for (value, condition) in [
        ("42", Scalar::Boolean(true)),
        ("{\"a\":1}", Scalar::Boolean(false)),
        ("null", Scalar::Null),
    ] {
        let value = Scalar::Variant(jsonb::parse_owned_jsonb(value.as_bytes()).unwrap().to_vec());
        json.push(value.as_ref());
        flag.push(condition.as_ref());
    }
    let block = DataBlock::new(vec![json.build().into(), flag.build().into()], 3);
    let base = "CASE WHEN flag THEN object_construct('a', v) WHEN flag IS NULL THEN NULL ELSE object_delete(v, 'missing') END";
    // OBJECT_DELETE must only run on row 1, not the scalar and JSON-null rows.
    for size in [0, 1, 3] {
        let block = block.slice(0..size);
        let reference = evaluate_block(base, "a", false, &block)?;
        assert_eq!(reference, evaluate_block(base, "a", true, &block)?);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn variant_path_rewrites_property_test() {
    init_testing_globals();
    let strategy = (any::<i64>(), any::<bool>(), any::<bool>());
    TestRunner::deterministic()
        .run(&strategy, |(number, has_key, flag)| {
            let value = if has_key {
                serde_json::json!({"a": {"b": number}, "secret": number})
            } else {
                serde_json::json!({"other": number})
            };
            let json = Scalar::Variant(
                jsonb::parse_owned_jsonb(value.to_string().as_bytes())
                    .unwrap()
                    .to_vec(),
            );
            for base in [
                "if(flag, v, object_delete(v, 'secret'))",
                "object_delete(object_construct('a', v), 'secret')",
            ] {
                for path in ["a", "secret", "a:b", "a:a:b"] {
                    compare_paths(base, path, json.clone(), Scalar::Boolean(flag));
                }
            }
            Ok(())
        })
        .unwrap();
}
