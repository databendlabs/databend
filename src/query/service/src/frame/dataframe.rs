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

use common_ast::ast::Expr;
use common_ast::ast::GroupBy;
use common_ast::ast::Indirection;
use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_ast::ast::OrderByExpr;
use common_ast::ast::SelectTarget;
use common_ast::ast::TableReference;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_sql::planner::optimizer::s_expr::SExpr;
use common_sql::plans::JoinType;
use common_sql::plans::Limit;
use common_sql::BindContext;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::NameResolutionContext;
use parking_lot::RwLock;

use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct Dataframe {
    query_ctx: Arc<QueryContext>,
    binder: Binder,
    bind_context: BindContext,
    s_expr: Option<SExpr>,
}

#[allow(dead_code)]
impl Dataframe {
    pub async fn new(
        query_ctx: Arc<QueryContext>,
        bind_context: Option<BindContext>,
        s_expr: Option<SExpr>,
    ) -> Result<Dataframe> {
        let settings = query_ctx.get_settings();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let binder = Binder::new(
            query_ctx.clone(),
            CatalogManager::instance(),
            name_resolution_ctx,
            metadata,
        );

        Ok(Dataframe {
            query_ctx,
            binder,
            bind_context: if let Some(bind_context) = bind_context {
                bind_context
            } else {
                BindContext::new()
            },
            s_expr,
        })
    }

    pub fn get_query_ctx(self) -> Arc<QueryContext> {
        self.query_ctx
    }
    pub fn get_expr(self) -> Option<SExpr> {
        self.s_expr
    }

    pub async fn select_one(mut self, select_list: Vec<SelectTarget>) -> Result<Dataframe> {
        let (s_expr, bind_context) = self
            .binder
            .bind_one_table(&self.bind_context, &select_list)
            .await?;
        Dataframe::new(self.query_ctx, Some(bind_context), Some(s_expr)).await
    }

    pub async fn select(mut self, from: Vec<TableReference>) -> Result<Dataframe> {
        let cross_joins = from
            .iter()
            .cloned()
            .reduce(|left, right| TableReference::Join {
                span: None,
                join: Join {
                    op: JoinOperator::CrossJoin,
                    condition: JoinCondition::None,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            })
            .unwrap();
        let (mut s_expr, mut bind_ctx) = self
            .binder
            .bind_table_reference(&mut self.bind_context, &cross_joins)
            .await?;
        let select_list: Vec<SelectTarget> = vec![SelectTarget::QualifiedName {
            qualified: vec![Indirection::Star(None)],
            exclude: None,
        }];
        let select_list = self
            .binder
            .normalize_select_list(&mut bind_ctx, &select_list)
            .await?;
        let (scalar_items, projections) = self
            .binder
            .analyze_projection(&self.bind_context.aggregate_info, &select_list)?;

        s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            s_expr,
        )?;
        s_expr = bind_ctx.add_internal_column_into_expr(s_expr);
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }

    pub async fn filter(mut self, expr: Expr) -> Result<Dataframe> {
        let (s_expr, _) = self
            .binder
            .bind_where(&mut self.bind_context, &[], &expr, self.s_expr.unwrap())
            .await?;
        Dataframe::new(self.query_ctx, None, Some(s_expr)).await
    }

    pub async fn aggregate(
        mut self,
        groupby: Option<GroupBy>,
        select_list: Vec<SelectTarget>,
        having: Option<Expr>,
    ) -> Result<Dataframe> {
        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())
            .await?;

        if let Some(group_by) = groupby {
            self.binder
                .analyze_group_items(&mut self.bind_context, &select_list, &group_by)
                .await?;
        }
        let mut s_expr = self.s_expr.clone().unwrap();
        if !self
            .bind_context
            .aggregate_info
            .aggregate_functions
            .is_empty()
            || !self.bind_context.aggregate_info.group_items.is_empty()
        {
            s_expr = self
                .binder
                .bind_aggregate(&mut self.bind_context, self.s_expr.unwrap())
                .await?;
        }
        let having = if let Some(having) = &having {
            Some(
                self.binder
                    .analyze_aggregate_having(&mut self.bind_context, &[], having)
                    .await?,
            )
        } else {
            None
        };
        if let Some((having, _)) = having {
            let s_expr = self
                .binder
                .bind_having(&mut self.bind_context, having, None, s_expr)
                .await?;
            Dataframe::new(self.query_ctx, Some(self.bind_context), Some(s_expr)).await
        } else {
            Dataframe::new(self.query_ctx, Some(self.bind_context), Some(s_expr)).await
        }
    }

    pub async fn distinct(mut self, select_list: Vec<SelectTarget>) -> Result<Dataframe> {
        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())
            .await?;
        let (mut scalar_items, projections) = self
            .binder
            .analyze_projection(&self.bind_context.aggregate_info, &select_list)?;
        let s_expr = self.binder.bind_distinct(
            None,
            &self.bind_context,
            &projections,
            &mut scalar_items,
            self.s_expr.unwrap(),
        )?;
        Dataframe::new(self.query_ctx, Some(self.bind_context), Some(s_expr)).await
    }

    pub async fn limit(self, limit: Option<usize>, offset: usize) -> Result<Dataframe> {
        let limit_plan = Limit { limit, offset };
        let s_expr =
            SExpr::create_unary(Arc::new(limit_plan.into()), Arc::new(self.s_expr.unwrap()));
        Dataframe::new(self.query_ctx, None, Some(s_expr)).await
    }

    pub async fn sort(
        mut self,
        select_list: Vec<SelectTarget>,
        order_by: &[OrderByExpr],
        distinct: bool,
    ) -> Result<Dataframe> {
        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())
            .await?;
        let (mut scalar_items, projections) = self
            .binder
            .analyze_projection(&self.bind_context.aggregate_info, &select_list)?;
        let order_items = self
            .binder
            .analyze_order_items(
                &mut self.bind_context,
                &mut scalar_items,
                &[],
                &projections,
                order_by,
                distinct,
            )
            .await?;
        let s_expr = self
            .binder
            .bind_order_by(
                &self.bind_context,
                order_items,
                &select_list,
                &mut scalar_items,
                self.s_expr.unwrap(),
            )
            .await?;
        Dataframe::new(self.query_ctx, Some(self.bind_context), Some(s_expr)).await
    }

    pub async fn select_columns(mut self, select_list: Vec<SelectTarget>) -> Result<Dataframe> {
        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())
            .await?;
        let (scalar_items, projections) = self
            .binder
            .analyze_projection(&self.bind_context.aggregate_info, &select_list)?;
        let s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr.unwrap(),
        )?;

        Dataframe::new(self.query_ctx, Some(self.bind_context), Some(s_expr)).await
    }

    pub async fn except(mut self, dataframe: Dataframe) -> Result<Dataframe> {
        let (s_expr, bind_ctx) = self.binder.bind_except(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr.unwrap(),
            dataframe.s_expr.unwrap(),
        )?;
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }

    pub async fn intersect(mut self, dataframe: Dataframe) -> Result<Dataframe> {
        let (s_expr, bind_ctx) = self.binder.bind_intersect(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr.unwrap(),
            dataframe.s_expr.unwrap(),
        )?;
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }

    pub async fn join(mut self, right: Dataframe, join_type: JoinType) -> Result<Dataframe> {
        let (s_expr, bind_ctx) = self.binder.bind_intersect_or_except(
            None,
            None,
            self.bind_context,
            right.bind_context,
            self.s_expr.unwrap(),
            right.s_expr.unwrap(),
            join_type,
        )?;
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }
    pub async fn union(mut self, dataframe: Dataframe) -> Result<Dataframe> {
        let (s_expr, bind_ctx) = self.binder.bind_union(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr.unwrap(),
            dataframe.s_expr.unwrap(),
            false,
        )?;
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }

    pub async fn union_distinct(mut self, dataframe: Dataframe) -> Result<Dataframe> {
        let (s_expr, bind_ctx) = self.binder.bind_union(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr.unwrap(),
            dataframe.s_expr.unwrap(),
            true,
        )?;
        Dataframe::new(self.query_ctx, Some(bind_ctx), Some(s_expr)).await
    }
}
