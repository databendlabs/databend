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
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::planner::optimize;
use databend_common_sql::planner::Binder;
use databend_common_sql::planner::Metadata;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Statistics;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;

/// A builder for setting test statistics on plans
#[derive(Clone, Debug, Default)]
pub struct TestStatisticsBuilder {
    // Map of table name to table statistics
    table_stats: HashMap<String, Option<TableStatistics>>,
    // Map of (table name, column name) to column statistics
    column_stats: HashMap<(String, String), Option<BasicColumnStatistics>>,
}

impl TestStatisticsBuilder {
    /// Create a new TestStatisticsBuilder
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            column_stats: HashMap::new(),
        }
    }

    /// Set statistics for a specific table
    pub fn set_table_statistics(
        &mut self,
        table_name: &str,
        stats: Option<TableStatistics>,
    ) -> &mut Self {
        self.table_stats.insert(table_name.to_string(), stats);
        self
    }

    /// Set statistics for a specific column in a table
    pub fn set_column_statistics(
        &mut self,
        table_name: &str,
        column_name: &str,
        stats: Option<BasicColumnStatistics>,
    ) -> &mut Self {
        self.column_stats
            .insert((table_name.to_string(), column_name.to_string()), stats);
        self
    }

    /// Apply the statistics to a plan
    pub fn apply_to_plan(&self, plan: &mut Plan) -> Result<()> {
        if let Plan::Query {
            s_expr, metadata, ..
        } = plan
        {
            let new_s_expr = self.apply_to_sexpr(s_expr, metadata)?;
            *s_expr = Box::new(new_s_expr);
        }
        Ok(())
    }

    /// Internal helper to apply statistics to an SExpr
    fn apply_to_sexpr(&self, s_expr: &SExpr, metadata: &MetadataRef) -> Result<SExpr> {
        let mut result = s_expr.clone();

        if let RelOperator::Scan(scan) = s_expr.plan.as_ref() {
            let table_index = scan.table_index;
            let metadata_guard = metadata.read();
            let table = metadata_guard.table(table_index);
            let table_name = table.name();

            // Check if we have statistics for this table
            let should_update = self.table_stats.contains_key(table_name)
                || self.column_stats.iter().any(|((t, _), _)| t == table_name);

            if should_update {
                let mut new_scan = scan.clone();

                // Create a mutable copy of the statistics
                let mut stats = Statistics {
                    table_stats: None,
                    column_stats: HashMap::new(),
                    histograms: HashMap::new(),
                };

                // Update table statistics if specified
                if let Some(table_stats) = self.table_stats.get(table_name) {
                    stats.table_stats = *table_stats;
                }

                // Update column statistics if specified
                let metadata_guard = metadata.read();
                let columns = metadata_guard.columns_by_table_index(table_index);
                for (idx, column) in columns.iter().enumerate() {
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) =
                        column
                    {
                        if let Some(col_stats_option) = self
                            .column_stats
                            .get(&(table_name.to_string(), column_name.clone()))
                        {
                            stats
                                .column_stats
                                .insert(idx as IndexType, col_stats_option.clone());
                        }
                    }
                }

                // Replace the statistics
                new_scan.statistics = Arc::new(stats);
                result = result.replace_plan(Arc::new(RelOperator::Scan(new_scan)));
            }
        }

        // Recursively process children
        let mut new_children = Vec::new();
        for child in s_expr.children() {
            new_children.push(Arc::new(self.apply_to_sexpr(child, metadata)?));
        }

        if !new_children.is_empty() {
            result = result.replace_children(new_children);
        }

        Ok(result)
    }
}

// Extension trait for Plan to make it easier to set statistics
pub trait PlanStatisticsExt {
    /// Set statistics for a specific table
    fn set_table_statistics(
        &mut self,
        table_name: &str,
        stats: Option<TableStatistics>,
    ) -> Result<&mut Self>;

    /// Set statistics for a specific column in a table
    fn set_column_statistics(
        &mut self,
        table_name: &str,
        column_name: &str,
        stats: Option<BasicColumnStatistics>,
    ) -> Result<&mut Self>;
}

impl PlanStatisticsExt for Plan {
    fn set_table_statistics(
        &mut self,
        table_name: &str,
        stats: Option<TableStatistics>,
    ) -> Result<&mut Self> {
        let mut builder = TestStatisticsBuilder::new();
        builder.set_table_statistics(table_name, stats);
        builder.apply_to_plan(self)?;
        Ok(self)
    }

    fn set_column_statistics(
        &mut self,
        table_name: &str,
        column_name: &str,
        stats: Option<BasicColumnStatistics>,
    ) -> Result<&mut Self> {
        let mut builder = TestStatisticsBuilder::new();
        builder.set_column_statistics(table_name, column_name, stats);
        builder.apply_to_plan(self)?;
        Ok(self)
    }
}

// TPC-DS Test Utilities

/// Plan SQL query to get a Plan object
pub async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    Ok(plan)
}

/// Execute SQL statement
pub async fn execute_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<()> {
    let plan = plan_sql(ctx, sql).await?;
    let it = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let _ = it.execute(ctx.clone()).await?;
    Ok(())
}

/// Get raw plan from SQL
pub async fn raw_plan(ctx: &Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let planner = Planner::new(ctx.clone());
    let extras = planner.parse_sql(sql)?;

    let metadata = Arc::new(parking_lot::RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::default();

    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata.clone(),
    );

    binder.bind(&extras.statement).await
}

/// Optimize a plan
pub async fn optimize_plan(ctx: &Arc<QueryContext>, plan: Plan) -> Result<Plan> {
    // Extract the metadata from the plan if it's a Query variant
    let metadata = match &plan {
        Plan::Query { metadata, .. } => metadata.clone(),
        _ => {
            // If it's not a Query, we still need to provide a metadata, but log a warning
            eprintln!("Warning: Plan is not a Query variant, creating new metadata");
            Arc::new(parking_lot::RwLock::new(Metadata::default()))
        }
    };

    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata);
    optimize(opt_ctx, plan).await
}

/// Test case structure for optimizer tests
pub struct TestCase {
    pub name: &'static str,
    pub sql: &'static str,
    pub stats_setup: fn(&mut Plan) -> Result<()>,
    pub raw_plan: &'static str,      // Expected raw plan string
    pub expected_plan: &'static str, // Expected optimized plan string
}
