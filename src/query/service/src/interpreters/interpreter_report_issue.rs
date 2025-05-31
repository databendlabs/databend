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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

use super::InterpreterFactory;
use super::ShowCreateQuerySettings;
use super::ShowCreateTableInterpreter;
use crate::interpreters::interpreter::auto_commit_if_not_allowed_in_transaction;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct ReportIssueInterpreter {
    sql: String,
    ctx: Arc<QueryContext>,
}

#[async_trait::async_trait]
impl Interpreter for ReportIssueInterpreter {
    fn name(&self) -> &str {
        "ReportIssueInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // Detection error
        // TODO: begin log collect
        let mut report_context = ReportContext::new(self.ctx.get_fuse_version());

        let mut planner = Planner::new_with_query_executor(
            self.ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                self.ctx.as_ref(),
            ))),
        );

        let Ok(mut extras) = planner.parse_sql(&self.sql) else {
            // TODO: Generate a bug report for the parser stage.
            return Err(ErrorCode::Unimplemented(
                "Bug Report: SQL Parsing Export Not Supported",
            ));
        };

        if let Err(_error) =
            auto_commit_if_not_allowed_in_transaction(self.ctx.clone(), &extras.statement).await
        {
            return Err(ErrorCode::Unimplemented(
                "Bug Report: auto_commit_if_not_allowed_in_transaction Export Not Supported",
            ));
        };

        let Ok(plan) = planner.plan_stmt(&extras.statement).await else {
            // TODO: Generate a bug report for the planner stage.
            return Err(ErrorCode::Unimplemented(
                "Bug Report: Plan statement Export Not Supported",
            ));
        };

        report_context
            .add_obfuscated_table_meta(self.ctx.clone(), &plan, &mut extras.statement)
            .await?;

        let Ok(interpreter) = InterpreterFactory::get(self.ctx.clone(), &plan).await else {
            // TODO: Generate a bug report for the interpreter stage.
            return Err(ErrorCode::Unimplemented(
                "Bug Report: Interpreter plan Export Not Supported",
            ));
        };

        let Ok(mut data_stream) = interpreter.execute(self.ctx.clone()).await else {
            // TODO: Generate a bug report for the interpreter stage.
            return Err(ErrorCode::Unimplemented(
                "Bug Report: Interpreter plan Export Not Supported",
            ));
        };

        while let Some(res) = data_stream.next().await {
            if let Err(cause) = res {
                report_context.add_report_error(cause);
                break;
            }
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(vec![format!("{}", report_context)]),
        ])])
    }
}

impl ReportIssueInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, sql: String) -> Result<Self> {
        Ok(ReportIssueInterpreter { ctx, sql })
    }
}

#[derive(VisitorMut)]
#[visitor(ColumnRef(enter), TableReference(enter))]
struct RewriteVisitor {
    mapping: HashMap<String, String>,
}

impl RewriteVisitor {
    fn enter_column_ref(&mut self, column_ref: &mut ColumnRef) {
        if let Some(v) = column_ref.database.as_mut() {
            if let Some(mapped_name) = self.mapping.get(&v.name) {
                v.name = mapped_name.clone();
            }
        }

        if let Some(v) = column_ref.table.as_mut() {
            if let Some(mapped_name) = self.mapping.get(&v.name) {
                v.name = mapped_name.clone();
            }
        }

        match &mut column_ref.column {
            ColumnID::Name(v) => {
                if let Some(mapped_name) = self.mapping.get(&v.name) {
                    v.name = mapped_name.clone();
                }
            }
            ColumnID::Position(_) => {}
        };
    }

    fn enter_table_reference(&mut self, table_ref: &mut TableReference) {
        if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_ref
        {
            if let Some(v) = catalog.as_mut() {
                if let Some(mapped_name) = self.mapping.get(&v.name) {
                    v.name = mapped_name.clone();
                }
            }

            if let Some(v) = database.as_mut() {
                if let Some(mapped_name) = self.mapping.get(&v.name) {
                    v.name = mapped_name.clone();
                }
            }

            if let Some(mapped_name) = self.mapping.get(&table.name) {
                table.name = mapped_name.clone();
            }
        }
    }
}

#[allow(dead_code)]
enum IssueType {
    Parser,
    Planner,
    Optimizer,
    MayPerformance,
}

struct TableStatisticsContext {
    name: String,
    statistics: TableStatistics,
    columns_statistics: HashMap<String, BasicColumnStatistics>,
}

struct ReportContext {
    typ: IssueType,
    version: String,
    replication_tables: HashMap<String, Vec<String>>,
    replication_databases: HashMap<String, String>,
    table_statistics: HashMap<String, TableStatisticsContext>,
    replication_queries: Vec<String>,

    error: Option<ErrorCode>,
    logs: Vec<String>,
}

impl ReportContext {
    pub fn new(version: String) -> ReportContext {
        ReportContext {
            version,
            typ: IssueType::MayPerformance,
            replication_tables: Default::default(),
            replication_databases: Default::default(),
            table_statistics: Default::default(),
            replication_queries: vec![],
            error: None,
            logs: vec![],
        }
    }

    pub fn add_report_error(&mut self, error_code: ErrorCode) {
        self.error = Some(error_code);
    }

    fn unique_name() -> String {
        format!("a{}", GlobalUniqName::unique())
    }

    pub async fn add_obfuscated_table_meta(
        &mut self,
        ctx: Arc<QueryContext>,
        plan: &Plan,
        statement: &mut Statement,
    ) -> Result<()> {
        let settings = ShowCreateQuerySettings {
            sql_dialect: Default::default(),
            force_quoted_ident: false,
            quoted_ident_case_sensitive: false,
            hide_options_in_show_create_table: true,
        };

        let mut mapping = HashMap::new();
        let mut statistics_tables = Vec::new();

        match &plan {
            Plan::Query { metadata, .. } => {
                let current_database = ctx.get_current_database();
                if !mapping.contains_key(&current_database) {
                    mapping.insert(current_database.clone(), Self::unique_name());
                }

                let current_database = mapping.get(&current_database).unwrap().clone();
                self.replication_databases.insert(
                    current_database.clone(),
                    format!("CREATE DATABASE IF NOT EXISTS {};", current_database),
                );

                for table in metadata.read().tables() {
                    if !mapping.contains_key(table.catalog()) {
                        mapping.insert(table.catalog().to_string(), Self::unique_name());
                    }

                    if !mapping.contains_key(table.database()) {
                        mapping.insert(table.database().to_string(), Self::unique_name());
                    }

                    if !mapping.contains_key(table.name()) {
                        mapping.insert(table.name().to_string(), Self::unique_name());
                    }

                    let mut table_info = table.table().get_table_info().clone();

                    if table_info.engine() == VIEW_ENGINE || table_info.engine() == STREAM_ENGINE {
                        return Err(ErrorCode::Unimplemented("Reports in SQL must not contain tables that use view or stream engines."));
                    }

                    table_info.meta.comment = String::new();
                    table_info.meta.field_comments =
                        vec!["".to_string(); table_info.schema().fields.len()];
                    table_info.name = mapping.get(table.name()).unwrap().clone();
                    table_info.desc = format!(
                        "{}.{}",
                        mapping.get(table.database()).unwrap(),
                        table_info.name
                    );

                    let mut table_schema = TableSchema::empty();

                    for field in table_info.schema().fields() {
                        if !mapping.contains_key(field.name()) {
                            mapping.insert(field.name().to_string(), Self::unique_name());
                        }

                        table_schema.fields.push(TableField::new(
                            mapping.get(field.name()).unwrap(),
                            field.data_type().clone(),
                        ));
                    }

                    let database_name = mapping.get(table.database()).unwrap();
                    if !self.replication_databases.contains_key(database_name) {
                        self.replication_databases.insert(
                            database_name.clone(),
                            format!("CREATE DATABASE IF NOT EXISTS {};", database_name),
                        );
                    }

                    if !self.replication_tables.contains_key(&table_info.desc) {
                        table_info = table_info.set_schema(Arc::new(table_schema));
                        let create_table = ShowCreateTableInterpreter::show_create_table_query(
                            &table_info,
                            &settings,
                        )?;

                        self.replication_tables
                            .insert(table_info.desc.clone(), vec![
                                format!("USE {}", database_name),
                                create_table,
                            ]);
                    }

                    statistics_tables.push((table_info.clone(), table.table().clone()));
                }

                self.replication_queries
                    .push(format!("USE {}", current_database));
            }
            _ => {
                // Add Note.
                // return Err(ErrorCode::Unimplemented("Only supports the report select statement."));
            }
        }

        for (info, statistics_table) in statistics_tables {
            let mut table_statistics_context = TableStatisticsContext {
                name: info.desc.clone(),
                statistics: Default::default(),
                columns_statistics: HashMap::new(),
            };

            if let Some(table_stats) = statistics_table
                .table_statistics(ctx.clone(), true, None)
                .await?
            {
                table_statistics_context.statistics = table_stats;
            }

            let column_statistics_provider = statistics_table
                .column_statistics_provider(ctx.clone())
                .await?;

            for field in statistics_table.schema().fields() {
                if let Some(column_statistics) =
                    column_statistics_provider.column_statistics(field.column_id)
                {
                    let column_name = mapping.get(field.name()).unwrap().clone();
                    table_statistics_context
                        .columns_statistics
                        .insert(column_name, column_statistics.clone());
                }
            }

            self.table_statistics
                .insert(info.desc.clone(), table_statistics_context);
        }

        let mut visitor = RewriteVisitor { mapping };
        statement.drive_mut(&mut visitor);
        self.replication_queries.push(format!("{}", statement));
        Ok(())
    }
}

use std::fmt;

use databend_common_base::base::GlobalUniqName;
use databend_common_sql::Planner;
use futures_util::StreamExt;

impl fmt::Display for IssueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IssueType::Parser => write!(f, "Parser"),
            IssueType::Planner => write!(f, "Planner"),
            IssueType::Optimizer => write!(f, "Optimizer"),
            IssueType::MayPerformance => write!(f, "MayPerformance"),
        }
    }
}

impl fmt::Display for TableStatisticsContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "### Table Statistics ({})", self.name)?;
        writeln!(f, "| Metric | Value |")?;
        writeln!(f, "|--------|-------|")?;

        if let Some(num_rows) = self.statistics.num_rows {
            writeln!(f, "| Number of Rows | {} |", num_rows)?;
        }

        if let Some(data_size) = self.statistics.data_size {
            writeln!(f, "| Data Size | {} |", data_size)?;
        }

        if let Some(compressed) = self.statistics.data_size_compressed {
            writeln!(f, "| Compressed Size | {} |", compressed)?;
        }

        if let Some(index_size) = self.statistics.index_size {
            writeln!(f, "| Index Size | {} |", index_size)?;
        }

        if let Some(blocks) = self.statistics.number_of_blocks {
            writeln!(f, "| Number of Blocks | {} |", blocks)?;
        }

        if let Some(segments) = self.statistics.number_of_segments {
            writeln!(f, "| Number of Segments | {} |", segments)?;
        }

        if !self.columns_statistics.is_empty() {
            writeln!(f, "\n#### Column Statistics")?;
            writeln!(f, "| Column | Min | Max | Distinct Values | Null Count |")?;
            writeln!(f, "|--------|-----|-----|-----------------|------------|")?;

            for (col_name, stats) in &self.columns_statistics {
                writeln!(
                    f,
                    "| {} | {} | {} | {} | {} |",
                    col_name,
                    stats
                        .min
                        .as_ref()
                        .map_or("NULL".to_string(), |d| format!("{}", d)),
                    stats
                        .max
                        .as_ref()
                        .map_or("NULL".to_string(), |d| format!("{}", d)),
                    stats.ndv.map_or("NULL".to_string(), |n| n.to_string()),
                    stats.null_count
                )?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for ReportContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "# Bug Report: {} Issue", self.typ)?;
        writeln!(f, "**Version**: {}\n", self.version)?;

        if !self.replication_databases.is_empty() {
            writeln!(f, "## Obfuscated Databases")?;
            writeln!(f, "```sql\n")?;
            for details in self.replication_databases.values() {
                writeln!(f, "{}", details)?;
            }
            writeln!(f, "```\n")?;
            writeln!(f)?;
        }

        if !self.replication_tables.is_empty() {
            writeln!(f, "## Obfuscated Tables")?;
            writeln!(f, "```sql\n")?;
            for (name, details) in &self.replication_tables {
                writeln!(f, "-- {}", name)?;
                for details in details {
                    writeln!(f, "{};", details)?;
                }
                writeln!(f)?;
            }
            writeln!(f, "```\n")?;
            writeln!(f)?;
        }

        if !self.table_statistics.is_empty() {
            writeln!(f, "## Obfuscated Table Statistics")?;
            for stats_ctx in self.table_statistics.values() {
                writeln!(f, "{}", stats_ctx)?;
            }
        }

        if !self.replication_queries.is_empty() {
            writeln!(f, "## Obfuscated Queries")?;
            writeln!(f, "```sql\n")?;
            for query in self.replication_queries.iter() {
                writeln!(f, "{};\n", query)?;
            }

            writeln!(f, "```\n")?;
        }

        if !self.logs.is_empty() {
            writeln!(f, "## Logs")?;
            for log in &self.logs {
                writeln!(f, "- {}", log)?;
            }
        }

        Ok(())
    }
}
