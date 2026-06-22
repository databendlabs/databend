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

use std::collections::HashMap;
use std::str::FromStr;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::FormatOptions;
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql_test_support::LiteTableContext;
use databend_common_sql_test_support::ReplayColumn;
use databend_common_sql_test_support::ReplayInput;
use databend_common_sql_test_support::ReplayScan;
use databend_common_sql_test_support::ReplayTable;
use databend_common_sql_test_support::ReplayUdf;
use databend_common_sql_test_support::ReplayView;
use serde::Deserialize;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<()> {
    let args = Args::parse()?;

    let input: Input = serde_json::from_reader(std::io::stdin().lock())
        .map_err(|err| ErrorCode::Internal(format!("invalid replay JSON: {err}")))?;
    let (sql, input) = input.into_parts();

    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    ctx.register_replay_input(&input).await?;

    let raw_plan = ctx.bind_sql(&sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;
    let plan = match args.explain_kind {
        ExplainKind::Optimized => optimized_plan.format_indent(FormatOptions::default())?,
        ExplainKind::Join => format_join_explain(optimized_plan)?,
    };
    println!("{plan}");

    Ok(())
}

#[derive(Debug)]
struct Args {
    explain_kind: ExplainKind,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut explain_kind = ExplainKind::Optimized;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--explain-kind" => {
                    let value = args.next().ok_or_else(|| {
                        ErrorCode::BadArguments("--explain-kind requires optimized or join")
                    })?;
                    explain_kind = value.parse()?;
                }
                "--help" | "-h" => {
                    println!("Usage: databend-planner-replay [--explain-kind optimized|join]");
                    std::process::exit(0);
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!("unknown argument `{arg}`")));
                }
            }
        }

        Ok(Self { explain_kind })
    }
}

#[derive(Debug, Clone, Copy)]
enum ExplainKind {
    Optimized,
    Join,
}

impl FromStr for ExplainKind {
    type Err = ErrorCode;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "optimized" => Ok(Self::Optimized),
            "join" => Ok(Self::Join),
            _ => Err(ErrorCode::BadArguments(format!(
                "unsupported --explain-kind `{value}`, expected optimized or join"
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Input {
    #[serde(alias = "query")]
    sql: String,
    #[serde(default)]
    views: Vec<ReplayView>,
    #[serde(default)]
    udfs: Vec<ReplayUdf>,
    table_stats: HashMap<usize, ReplayTable>,
    column_stats: Vec<ReplayColumn>,
    scan_mappings: Vec<ReplayScan>,
}

impl Input {
    fn into_parts(self) -> (String, ReplayInput) {
        (self.sql, ReplayInput {
            views: self.views,
            udfs: self.udfs,
            table_stats: self.table_stats,
            column_stats: self.column_stats,
            scan_mappings: self.scan_mappings,
        })
    }
}

fn format_join_explain(plan: Plan) -> Result<String> {
    let Plan::Query {
        s_expr, metadata, ..
    } = plan
    else {
        return Err(ErrorCode::Unimplemented(
            "Unsupported --explain-kind join for non-query plan",
        ));
    };

    Ok(join_format_tree(&s_expr, &metadata)?.format_pretty()?)
}

fn join_format_tree(s_expr: &SExpr, metadata: &MetadataRef) -> Result<FormatTreeNode> {
    match s_expr.plan() {
        RelOperator::Join(join) => {
            let build_child =
                FormatTreeNode::with_children("Build".to_string(), vec![join_format_tree(
                    s_expr.build_side_child(),
                    metadata,
                )?]);
            let probe_child =
                FormatTreeNode::with_children("Probe".to_string(), vec![join_format_tree(
                    s_expr.probe_side_child(),
                    metadata,
                )?]);
            Ok(FormatTreeNode::with_children(
                format!("HashJoin: {}", join.join_type),
                vec![build_child, probe_child],
            ))
        }
        RelOperator::Scan(scan) => {
            let metadata = metadata.read();
            let table = metadata.table(scan.table_index);
            let read_rows = scan
                .statistics
                .table_stats
                .as_ref()
                .and_then(|stats| stats.num_rows)
                .map_or_else(|| "None".to_string(), |rows| rows.to_string());
            Ok(FormatTreeNode::new(format!(
                "Scan: {} (#{}) (read rows: {})",
                table.qualified_name(),
                scan.table_index,
                read_rows
            )))
        }
        _ => {
            let children = s_expr
                .children()
                .map(|child| join_format_tree(child, metadata))
                .collect::<Result<Vec<_>>>()?;

            if children.len() == 1 {
                Ok(children.into_iter().next().unwrap())
            } else {
                Ok(FormatTreeNode::with_children(
                    format!("{:?}", s_expr.plan().rel_op()),
                    children,
                ))
            }
        }
    }
}
