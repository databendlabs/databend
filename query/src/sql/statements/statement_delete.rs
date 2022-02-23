// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::Expression;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::analyzer_statement::QueryAnalyzeState;
use crate::sql::statements::delete::DeleteNormalizer;
use crate::sql::statements::delete::DeleteSchemaAnalyzer;
use crate::sql::statements::query::JoinedSchema;
use crate::sql::statements::query::JoinedTableDesc;
use crate::sql::statements::query::QualifiedRewriter;
use crate::sql::statements::query::QueryASTIR;
use crate::sql::statements::query::QueryCollectPushDowns;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::QueryRelation;
use crate::storages::ToReadDataSourcePlan;

#[derive(Debug, Clone, PartialEq)]
pub struct DfDeleteStatement {
    pub from: ObjectName,
    pub selection: Option<Expr>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDeleteStatement {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let analyzer = DeleteSchemaAnalyzer::create(ctx.clone());
        let mut joined_schema = analyzer.analyze(self).await?;

        let mut ir = DeleteNormalizer::normalize(ctx.clone(), self).await?;

        QualifiedRewriter::rewrite(&joined_schema, ctx.clone(), &mut ir)?;

        QueryCollectPushDowns::collect_extras(&mut ir, &mut joined_schema)?;

        let analyze_state = self.analyze_query(ir).await?;
        self.check_and_finalize(joined_schema, analyze_state, ctx)
            .await
    }
}

impl DfDeleteStatement {
    async fn analyze_query(&self, ir: QueryASTIR) -> Result<QueryAnalyzeState> {
        let limit = ir.limit;
        let offset = ir.offset;
        let mut analyze_state = QueryAnalyzeState {
            limit,
            offset,
            ..Default::default()
        };
        if let Some(predicate) = &ir.filter_predicate {
            Self::verify_no_aggregate(predicate, "filter")?;
            analyze_state.filter = Some(predicate.clone());
        }
        Ok(analyze_state)
    }

    fn verify_no_aggregate(expr: &Expression, info: &str) -> Result<()> {
        match find_aggregate_exprs_in_expr(expr).is_empty() {
            true => Ok(()),
            false => Err(ErrorCode::SyntaxException(format!(
                "{} cannot contain aggregate functions",
                info
            ))),
        }
    }
}

impl DfDeleteStatement {
    pub async fn check_and_finalize(
        &self,
        schema: JoinedSchema,
        mut state: QueryAnalyzeState,
        ctx: Arc<QueryContext>,
    ) -> Result<AnalyzedResult> {
        let dry_run_res = Self::verify_with_dry_run(&schema, &state)?;
        state.finalize_schema = dry_run_res.schema().clone();

        let mut tables_desc = schema.take_tables_desc();

        if tables_desc.len() != 1 {
            return Err(ErrorCode::UnImplement("Select join unimplemented yet."));
        }

        match tables_desc.remove(0) {
            JoinedTableDesc::Table {
                table, push_downs, ..
            } => {
                let source_plan = table.read_plan(ctx.clone(), push_downs).await?;
                state.relation = QueryRelation::FromTable(Box::new(source_plan));
            }
            JoinedTableDesc::Subquery {
                state: subquery_state,
                ..
            } => {
                // TODO: maybe need reanalyze subquery.
                state.relation = QueryRelation::Nested(subquery_state);
            }
        }

        Ok(AnalyzedResult::DeleteStatement(Box::new(state)))
    }

    fn verify_with_dry_run(schema: &JoinedSchema, state: &QueryAnalyzeState) -> Result<DataBlock> {
        let data_block = DataBlock::empty_with_schema(schema.to_data_schema());

        if let Some(predicate) = &state.filter {
            if let Err(cause) = Self::dry_run_expr(predicate, &data_block) {
                return Err(cause.add_message_back(" (while in delete filter)"));
            }
        }

        Ok(data_block)
    }

    fn dry_run_expr(expr: &Expression, data: &DataBlock) -> Result<DataBlock> {
        let schema = data.schema();
        let data_field = expr.to_data_field(schema)?;
        Ok(DataBlock::empty_with_schema(DataSchemaRefExt::create(
            vec![data_field],
        )))
    }
}
