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

use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::optimizer::ir::Statistics;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::Scan;

use crate::framework::LiteTableContext;

fn column(index: usize) -> ScalarExpr {
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("c{index}"),
            Symbol::new(index),
            Box::new(DataType::Nullable(Box::new(DataType::Number(
                NumberDataType::UInt64,
            )))),
            Visibility::Visible,
        )
        .build(),
    })
}

fn stat_leaf(cardinality: f64) -> SExpr {
    SExpr::create(
        Scan::default(),
        vec![],
        None,
        None,
        Some(Arc::new(StatInfo {
            cardinality,
            max_cardinality: cardinality,
            statistics: Statistics::default(),
        })),
    )
}

fn nullable_mark_join(join_type: JoinType) -> Join {
    Join {
        equi_conditions: vec![JoinEquiCondition::new(column(0), column(1), true)],
        join_type,
        ..Default::default()
    }
}

fn required_distributions(
    ctx: Arc<dyn TableContext>,
    join_type: JoinType,
    left_cardinality: f64,
    right_cardinality: f64,
) -> Result<Vec<Vec<Distribution>>> {
    let join = nullable_mark_join(join_type);
    let s_expr = SExpr::create_binary(
        join.clone(),
        stat_leaf(left_cardinality),
        stat_leaf(right_cardinality),
    );
    let rel_expr = RelExpr::with_s_expr(&s_expr);

    Ok(join
        .compute_required_prop_children(ctx, &rel_expr, &RequiredProperty::default())?
        .into_iter()
        .map(|properties| {
            properties
                .into_iter()
                .map(|property| property.distribution)
                .collect()
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_nullable_mark_join_broadcast_guard() -> Result<()> {
    const SAFE_ROWS: f64 = 10.0;
    const UNSAFE_ROWS: f64 = 200_000_000.0;

    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(3);
    let settings = ctx.get_settings();
    settings.set_setting(
        "max_broadcast_join_build_rows".to_string(),
        "100000000".to_string(),
    )?;

    let left_hash = Distribution::GlobalHash(vec![column(0)]);
    let right_hash = Distribution::GlobalHash(vec![column(1)]);
    let serial = vec![vec![Distribution::Serial, Distribution::Serial]];

    assert_eq!(
        required_distributions(ctx.clone(), JoinType::LeftMark, SAFE_ROWS, UNSAFE_ROWS)?,
        vec![vec![Distribution::Broadcast, right_hash.clone()]],
    );
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::RightMark, UNSAFE_ROWS, SAFE_ROWS)?,
        vec![vec![left_hash.clone(), Distribution::Broadcast]],
    );
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::LeftMark, UNSAFE_ROWS, SAFE_ROWS)?,
        serial,
    );
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::RightMark, SAFE_ROWS, UNSAFE_ROWS)?,
        serial,
    );

    settings.set_setting("enforce_broadcast_join".to_string(), "1".to_string())?;
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::LeftMark, UNSAFE_ROWS, SAFE_ROWS)?,
        vec![vec![Distribution::Broadcast, right_hash.clone()]],
    );
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::RightMark, SAFE_ROWS, UNSAFE_ROWS)?,
        vec![vec![left_hash.clone(), Distribution::Broadcast]],
    );

    settings.set_setting("enforce_broadcast_join".to_string(), "0".to_string())?;
    ctx.set_cluster_node_num(1);
    assert_eq!(
        required_distributions(ctx.clone(), JoinType::LeftMark, UNSAFE_ROWS, SAFE_ROWS)?,
        vec![vec![Distribution::Broadcast, right_hash]],
    );
    assert_eq!(
        required_distributions(ctx, JoinType::RightMark, SAFE_ROWS, UNSAFE_ROWS)?,
        vec![vec![left_hash, Distribution::Broadcast]],
    );

    Ok(())
}
