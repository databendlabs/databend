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

use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Symbol;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_sql::format_scalar;
use parking_lot::RwLock;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

async fn type_check_case(case: &SqlTestCase) -> Result<SqlTestOutcome> {
    let ctx = setup_context(case).await?;
    let tokens = tokenize_sql(case.sql)?;
    let dialect = ctx.get_settings().get_sql_dialect()?;
    let expr = parse_expr(&tokens, dialect)?;

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut bind_context = BindContext::new();
    bind_context.add_column_binding(
        ColumnBindingBuilder::new(
            "number".to_string(),
            Symbol::new(0),
            Box::new(DataType::Number(NumberDataType::Int64).wrap_nullable()),
            Visibility::Visible,
        )
        .build(),
    );
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    let outcome = match type_checker.resolve(&expr).map(|resolved| *resolved) {
        Ok((scalar, data_type)) => SqlTestOutcome::Plan(format!(
            "scalar: {}\ntype: {}",
            format_scalar(&scalar),
            data_type
        )),
        Err(err) => SqlTestOutcome::Error {
            code: err.code(),
            message: err.message(),
        },
    };
    Ok(outcome)
}

async fn run_type_check_cases(file_name: &str, cases: &[SqlTestCase]) -> Result<()> {
    let mut file = open_golden_file("semantic", file_name)?;

    for case in cases {
        write_case_header(&mut file, case)?;
        let outcome = type_check_case(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_core_lowering() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "minus_literal_is_resolved_during_lowering",
            description: "Unary minus on a literal should become a negative literal before scalar function resolution.",
            setup_sqls: &[],
            sql: "-1",
        },
        SqlTestCase {
            name: "minus_non_literal_remains_scalar_operator",
            description: "Unary minus on a non-literal should still resolve through the scalar operator path.",
            setup_sqls: &[],
            sql: "-number",
        },
        SqlTestCase {
            name: "is_null_wrapper_lowers_as_boolean_expression",
            description: "IS NULL carries its own wrapper semantics and should not collapse to the child expression.",
            setup_sqls: &[],
            sql: "number IS NULL",
        },
        SqlTestCase {
            name: "between_expression_lowers_to_comparison_core",
            description: "BETWEEN should type check after lowering into comparison expressions.",
            setup_sqls: &[],
            sql: "number BETWEEN 1 AND 5",
        },
        SqlTestCase {
            name: "aggregate_function_uses_aggregate_core_path",
            description: "A plain aggregate call should be separated from scalar calls during CoreExpr lowering.",
            setup_sqls: &[],
            sql: "sum(number)",
        },
        SqlTestCase {
            name: "aggregate_window_function_uses_window_core_path",
            description: "An aggregate with OVER should resolve as a window expression, not as a grouped aggregate.",
            setup_sqls: &[],
            sql: "sum(number) OVER ()",
        },
        SqlTestCase {
            name: "general_window_function_uses_window_core_path",
            description: "A general window function with OVER should resolve through the window CoreExpr branch.",
            setup_sqls: &[],
            sql: "row_number() OVER ()",
        },
        SqlTestCase {
            name: "general_window_function_without_over_errors_early",
            description: "A general window function without OVER should fail from the CoreExpr window split.",
            setup_sqls: &[],
            sql: "row_number()",
        },
    ];

    run_type_check_cases("type_check_core_lowering.txt", &cases).await
}
