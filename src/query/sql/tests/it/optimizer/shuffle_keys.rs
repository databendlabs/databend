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

use std::io::Write;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::CastExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::Scan;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;

fn column(index: usize, data_type: DataType) -> ScalarExpr {
    BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("key{index}"),
            Symbol::new(index),
            Box::new(data_type),
            Visibility::Visible,
        )
        .build(),
    }
    .into()
}

fn call(name: &str, args: Vec<ScalarExpr>, data_type: DataType) -> ScalarExpr {
    FunctionCall {
        span: None,
        func_name: name.to_string(),
        params: vec![],
        arguments: args,
        return_type: Box::new(data_type),
    }
    .into()
}

fn cast(arg: ScalarExpr, target: DataType, is_try: bool) -> ScalarExpr {
    CastExpr {
        span: None,
        is_try,
        argument: Box::new(arg),
        target_type: Box::new(target),
    }
    .into()
}

fn pair(left: ScalarExpr, right: ScalarExpr) -> JoinEquiCondition {
    JoinEquiCondition::new(left, right, false)
}

fn direct_pair(data_type: DataType) -> JoinEquiCondition {
    pair(column(3, data_type.clone()), column(6, data_type))
}

fn extra_pair() -> JoinEquiCondition {
    pair(column(4, DataType::Boolean), column(7, DataType::Boolean))
}

// Check the complete requirements from both entry points, not just a private selection helper.
fn assert_keys(
    ctx: Arc<dyn TableContext>,
    label: &str,
    conditions: Vec<JoinEquiCondition>,
    indices: &[usize],
) -> Result<()> {
    let expected = vec![
        RequiredProperty {
            distribution: Distribution::GlobalHash(
                indices
                    .iter()
                    .map(|&i| conditions[i].left.clone())
                    .collect(),
            ),
        },
        RequiredProperty {
            distribution: Distribution::GlobalHash(
                indices
                    .iter()
                    .map(|&i| conditions[i].right.clone())
                    .collect(),
            ),
        },
    ];
    let join = Join {
        equi_conditions: conditions,
        join_type: JoinType::Inner,
        ..Default::default()
    };
    let input = || {
        PExpr::create_unary(
            Exchange::GlobalHash(vec![]),
            PExpr::create_leaf(Scan::default()),
        )
    };
    let plan = PExpr::create_binary(join.clone(), input(), input());
    let rel = RelExpr::with_p_expr(&plan);
    let required = RequiredProperty {
        distribution: Distribution::Any,
    };
    assert_eq!(
        join.compute_required_prop_children(ctx.clone(), &rel, &required)?,
        vec![expected.clone()],
        "{label}: exploration"
    );
    for (side, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            join.compute_required_prop_child(ctx.clone(), &rel, side, &required)?,
            expected,
            "{label}: child {side}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_paired_shuffle_key_selection() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(2);
    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "1".to_string())?;
    let integer = DataType::Number(NumberDataType::Int32);
    let wide = DataType::Number(NumberDataType::Int64);
    let length = |index| {
        call(
            "length",
            vec![column(index, DataType::String)],
            DataType::Number(NumberDataType::UInt64),
        )
    };
    let derived = pair(length(3), length(6));
    assert_keys(
        ctx.clone(),
        "paired derived keys",
        vec![derived.clone(), direct_pair(DataType::String), extra_pair()],
        &[1, 2],
    )?;
    let text = |name, index| {
        call(
            name,
            vec![column(index, DataType::String)],
            DataType::String,
        )
    };
    assert_keys(
        ctx.clone(),
        "consecutive pruning stops at two keys and preserves order",
        vec![
            pair(text("lower", 3), text("lower", 6)),
            direct_pair(DataType::String),
            pair(text("upper", 3), text("upper", 6)),
            derived,
        ],
        &[1, 3],
    )?;
    assert_keys(
        ctx.clone(),
        "single key",
        vec![direct_pair(integer.clone())],
        &[0],
    )?;
    for nullable in [false, true] {
        let source = if nullable {
            integer.clone().wrap_nullable()
        } else {
            integer.clone()
        };
        let target = if nullable {
            wide.clone().wrap_nullable()
        } else {
            wide.clone()
        };
        let widening = pair(
            cast(column(3, source.clone()), target.clone(), false),
            cast(column(6, source.clone()), target, false),
        );
        assert_keys(
            ctx.clone(),
            "lossless integer cast including nullable input",
            vec![direct_pair(source), widening, extra_pair()],
            &[0, 2],
        )?;
    }
    assert_keys(
        ctx.clone(),
        "uncovered right-side dependency",
        vec![
            pair(length(3), length(8)),
            direct_pair(DataType::String),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    assert_keys(
        ctx.clone(),
        "one-sided redundancy",
        vec![
            pair(
                length(3),
                column(8, DataType::Number(NumberDataType::UInt64)),
            ),
            direct_pair(DataType::String),
            pair(
                column(4, DataType::Number(NumberDataType::UInt64)),
                column(8, DataType::Number(NumberDataType::UInt64)),
            ),
        ],
        &[0, 1, 2],
    )?;
    for (source, target, is_try) in [
        (wide.clone(), integer.clone(), false),
        (integer.clone(), wide.clone(), true),
        (DataType::String, integer.clone(), false),
        (DataType::Date, DataType::Timestamp, false),
        (integer.clone().wrap_nullable(), wide.clone(), false),
    ] {
        assert_keys(
            ctx.clone(),
            "unsafe or context-sensitive cast",
            vec![
                direct_pair(source.clone()),
                pair(
                    cast(column(3, source.clone()), target.clone(), is_try),
                    cast(column(6, source), target, is_try),
                ),
                extra_pair(),
            ],
            &[0, 1, 2],
        )?;
    }
    for source in [
        DataType::Timestamp,
        DataType::Number(NumberDataType::Float64),
    ] {
        let name = if source == DataType::Timestamp {
            "to_date"
        } else {
            "abs"
        };
        let result = if source == DataType::Timestamp {
            DataType::Date
        } else {
            source.clone()
        };
        assert_keys(
            ctx.clone(),
            "context dependency or non-exact source equality",
            vec![
                direct_pair(source.clone()),
                pair(
                    call(name, vec![column(3, source.clone())], result.clone()),
                    call(name, vec![column(6, source)], result),
                ),
                extra_pair(),
            ],
            &[0, 1, 2],
        )?;
    }
    let unsigned = DataType::Number(NumberDataType::UInt64);
    let random = |index| {
        call(
            "rand",
            vec![column(index, unsigned.clone())],
            DataType::Number(NumberDataType::Float64),
        )
    };
    assert_keys(
        ctx.clone(),
        "non-deterministic function",
        vec![
            direct_pair(unsigned.clone()),
            pair(random(3), random(6)),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    let sum = |a, b| {
        call(
            "plus",
            vec![column(a, integer.clone()), column(b, integer.clone())],
            wide.clone(),
        )
    };
    assert_keys(
        ctx.clone(),
        "multiple dependencies remain conservative",
        vec![
            direct_pair(integer.clone()),
            pair(sum(3, 4), sum(6, 7)),
            pair(column(4, integer.clone()), column(7, integer.clone())),
        ],
        &[0, 1, 2],
    )?;
    let nullable = integer.wrap_nullable();
    let mut null_direct = direct_pair(nullable.clone());
    null_direct.is_null_equal = true;
    let mut null_derived = pair(
        cast(column(3, nullable.clone()), wide.wrap_nullable(), false),
        cast(column(6, nullable.clone()), wide.wrap_nullable(), false),
    );
    null_derived.is_null_equal = true;
    assert_keys(
        ctx.clone(),
        "null-safe equality",
        vec![null_direct, null_derived, extra_pair()],
        &[0, 2],
    )?;
    assert_keys(
        ctx.clone(),
        "different direct key nullability",
        vec![
            pair(column(3, integer.clone()), column(6, nullable.clone())),
            pair(
                cast(column(3, integer.clone()), wide.clone(), false),
                cast(column(6, nullable), wide.wrap_nullable(), false),
            ),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    assert_keys(
        ctx.clone(),
        "timestamp offsets are not exact equality",
        vec![
            direct_pair(DataType::TimestampTz),
            pair(
                call(
                    "to_string",
                    vec![column(3, DataType::TimestampTz)],
                    DataType::String,
                ),
                call(
                    "to_string",
                    vec![column(6, DataType::TimestampTz)],
                    DataType::String,
                ),
            ),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    assert_keys(
        ctx.clone(),
        "unknown function",
        vec![
            direct_pair(integer.clone()),
            pair(
                call(
                    "unknown_shuffle_function",
                    vec![column(3, integer.clone())],
                    integer.clone(),
                ),
                call(
                    "unknown_shuffle_function",
                    vec![column(6, integer.clone())],
                    integer.clone(),
                ),
            ),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    assert_keys(
        ctx.clone(),
        "different direct key types",
        vec![
            pair(column(3, integer.clone()), column(6, wide.clone())),
            pair(
                cast(column(3, integer), wide.clone(), false),
                cast(column(6, wide.clone()), wide, false),
            ),
            extra_pair(),
        ],
        &[0, 1, 2],
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_derived_shuffle_keys_sql_plan() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_table_warehouse_distribution(true);
    ctx.set_cluster_node_num(2);
    for setting in ["enforce_shuffle_join", "disable_join_reorder"] {
        ctx.get_settings()
            .set_setting(setting.to_string(), "1".to_string())?;
    }
    for name in ["shuffle_left", "shuffle_right"] {
        ctx.register_setup_sql(&format!(
            "CREATE TABLE {name} (k INT, flag INT, tag STRING)"
        ))
        .await?;
    }
    let sql = "SELECT l.k, l.flag FROM shuffle_left l JOIN shuffle_right r \
               ON l.tag = r.tag AND lower(l.tag) = lower(r.tag) AND l.flag = r.flag";
    let plan = ctx.optimize_plan(ctx.bind_sql(sql).await?).await?;
    let mut file = open_golden_file("optimizer", "shuffle_keys.txt")?;
    // The complete plan must retain the derived join predicate but omit it from both exchanges.
    writeln!(file, "sql: {sql}")?;
    writeln!(
        file,
        "{}",
        plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    Ok(())
}
