use crate::sql::analyzer::{AnalyzableStatement, AnalyzedResult};
use sqlparser::ast::{Query, SetExpr, Select, TableWithJoins};
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use common_exception::{Result, ErrorCode};
use crate::catalogs::ToReadDataSourcePlan;
use std::sync::Arc;

pub struct SelectQueryAnalyzedResult {}

#[async_trait::async_trait]
impl AnalyzableStatement for Box<Query> {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        if let SetExpr::Select(query) = &self.body {
            let query: &Select = query.as_ref();
            if self.with.is_some() {
                return Result::Err(ErrorCode::UnImplement("CTE is not yet implement"));
            }

            let mut data = AnalyzerData { ctx, res: SelectQueryAnalyzedResult {} };

            if let Err(cause) = SelectAnalyzer::analyze_from(&query.from, &mut data).await {
                return Err(cause.add_message_back("(while in analyze select)."));
            }

            unimplemented!("")
        } else {
            Result::Err(ErrorCode::UnImplement(format!(
                "Query {} is not yet implemented",
                self.body
            )))
        }
    }
}

struct AnalyzerData {
    ctx: DatabendQueryContextRef,
    res: SelectQueryAnalyzedResult,
}

struct SelectAnalyzer {
    res: SelectQueryAnalyzedResult,
}

impl SelectAnalyzer {
    pub async fn analyze_from(expr: &[TableWithJoins], data: &mut AnalyzerData) -> Result<()> {
        match expr.len() {
            0 => Self::append_dummy_source(&expr[0], data).await,
            1 => Self::plan_table_with_joins(&from[0]),
            // Such as SELECT * FROM t1, t2;
            // It's not `JOIN` clause.
            _ => Result::Err(ErrorCode::SyntaxException("Cannot SELECT multiple tables")),
        }
    }

    fn append_dummy_source(expr: &TableWithJoins, data: &mut AnalyzerData) -> Result<()> {
        let dummy_table = data.ctx.get_table("system", "one")?;

        let dummy_table_id = dummy_table.get_id();
        let dummy_table_version = Some(dummy_table.get_table_info().ident.version);

        let tbl_scan_info = TableScanInfo {
            table_name: "one",
            table_id,
            table_version,
            table_schema: &table.schema(),
            table_args: None,
        };

        let max_threads = data.get_settings().get_max_threads()? as usize;
        let io_ctx = Arc::new(data.ctx.get_single_node_table_io_context()?);
        let read_plan = dummy_table.read_plan(io_ctx, None, Some(max_threads));
        data.res. = read_plan.await?;
        PlanBuilder::scan("system", tbl_scan_info, None, None)
            .and_then(|builder| builder.build())
            .and_then(|dummy_scan_plan| match dummy_scan_plan {
                PlanNode::Scan(ref dummy_scan_plan) => {
                    // TODO(xp): is it possible to use get_cluster_table_io_context() here?
                    let io_ctx = data.get_single_node_table_io_context()?;
                    dummy_table.read_plan(
                        Arc::new(io_ctx),
                        None,
                        Some(data.get_settings().get_max_threads()? as usize),
                    )
                        .await
                        .map(PlanNode::ReadSource)
                }
                _unreachable_plan => panic!("Logical error: cannot downcast to scan plan"),
            })
    }
}
