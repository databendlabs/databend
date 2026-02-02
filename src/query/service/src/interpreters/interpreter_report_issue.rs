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
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use concurrent_queue::ConcurrentQueue;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::StringType;
use databend_common_settings::Settings;
use databend_common_sql::Planner;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use futures_util::StreamExt;
use log::LevelFilter;

use super::InterpreterFactory;
use super::ShowCreateQuerySettings;
use super::ShowCreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::interpreter::auto_commit_if_not_allowed_in_transaction;
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
        let mut report_context = ReportContext::new(self.ctx.get_fuse_version());
        let settings = self.ctx.get_settings();
        report_context.add_setting_changes(settings);

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_query(
            LevelFilter::Debug,
            report_context.logs.clone(),
        ));

        tracking_payload
            .tracking(self.detection_error(&mut report_context))
            .await?;

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(vec![format!("{}", report_context)]),
        ])])
    }
}

impl ReportIssueInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, sql: String) -> Result<Self> {
        Ok(ReportIssueInterpreter { ctx, sql })
    }

    async fn detection_error(&self, report_context: &mut ReportContext) -> Result<()> {
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

        let Ok(plan) = planner.plan_stmt(&extras.statement, false).await else {
            // TODO: Generate a bug report for the planner stage.
            return Err(ErrorCode::Unimplemented(
                "Bug Report: Plan statement Export Not Supported",
            ));
        };

        report_context
            .add_obfuscated_table_meta(self.ctx.clone(), &plan, &mut extras.statement)
            .await?;

        let interpreter = match InterpreterFactory::get(self.ctx.clone(), &plan).await {
            Ok(interpreter) => interpreter,
            Err(error) => {
                report_context.add_report_error(error);
                report_context.typ = IssueType::Planner;
                return Ok(());
            }
        };

        let mut data_stream = match interpreter.execute(self.ctx.clone()).await {
            Ok(data_stream) => data_stream,
            Err(error) => {
                report_context.add_report_error(error);
                report_context.typ = IssueType::Planner;
                return Ok(());
            }
        };

        while let Some(res) = data_stream.next().await {
            if let Err(cause) = res {
                report_context.add_report_error(cause);
                break;
            }
        }

        Ok(())
    }
}

#[derive(VisitorMut)]
#[visitor(Identifier(enter))]
struct CollectIdentifiersVisitor {
    index: usize,
    identifiers: HashSet<String>,
}

impl CollectIdentifiersVisitor {
    fn enter_identifier(&mut self, identifier: &mut Identifier) {
        self.identifiers.insert(identifier.name.clone());
    }

    pub fn new() -> CollectIdentifiersVisitor {
        CollectIdentifiersVisitor {
            index: 1296,
            identifiers: Default::default(),
        }
    }

    fn next_unique_name(&mut self) -> String {
        let mut result = Vec::new();
        let digits: Vec<char> = "0123456789abcdefghijklmnopqrstuvwxyz".chars().collect();

        let remainder = self.index % 36;
        let needed_increment = 10_usize.saturating_sub(remainder);
        self.index += needed_increment;

        let mut index = self.index;

        while index > 0 {
            let remainder = index % 36;
            result.push(digits[remainder]);
            index /= 36;
        }

        self.index += 1;
        result.into_iter().rev().collect()
    }

    pub fn decimal_to_base36_letter_prefix(&mut self) -> String {
        let index = self.index;
        let base36 = self.next_unique_name();

        if let Some(first_char) = base36.chars().next() {
            if first_char.is_ascii_lowercase() {
                return base36;
            }
        }

        let digits: Vec<char> = "0123456789abcdefghijklmnopqrstuvwxyz".chars().collect();
        let first_digit_value = digits
            .iter()
            .position(|&c| c == base36.chars().next().unwrap())
            .unwrap();

        let power = 36_usize.pow((base36.len() - 1) as u32);
        let needed_increment = (10 - first_digit_value) * power;

        self.index = index + needed_increment;
        self.next_unique_name()
    }

    fn unique_name(&mut self) -> String {
        loop {
            let next_unique_name = self.decimal_to_base36_letter_prefix();

            if !self.identifiers.contains(&next_unique_name) {
                return next_unique_name;
            }
        }
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
        if let TableReference::Table { table, .. } = table_ref {
            if let Some(v) = table.catalog.as_mut() {
                if let Some(mapped_name) = self.mapping.get(&v.name) {
                    v.name = mapped_name.clone();
                }
            }

            if let Some(v) = table.database.as_mut() {
                if let Some(mapped_name) = self.mapping.get(&v.name) {
                    v.name = mapped_name.clone();
                }
            }

            if let Some(mapped_name) = self.mapping.get(&table.table.name) {
                table.table.name = mapped_name.clone();
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
    setting_changes: Option<Arc<Settings>>,

    error: Option<ErrorCode>,
    logs: Arc<ConcurrentQueue<String>>,
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
            setting_changes: None,
            error: None,
            logs: Arc::new(ConcurrentQueue::unbounded()),
        }
    }

    pub fn add_setting_changes(&mut self, changes: Arc<Settings>) {
        self.setting_changes = Some(changes);
    }

    pub fn add_report_error(&mut self, error_code: ErrorCode) {
        self.error = Some(error_code);
    }

    pub async fn add_obfuscated_table_meta(
        &mut self,
        ctx: Arc<QueryContext>,
        plan: &Plan,
        statement: &mut Statement,
    ) -> Result<()> {
        let mut visitor = CollectIdentifiersVisitor::new();
        statement.drive_mut(&mut visitor);

        let settings = ShowCreateQuerySettings {
            sql_dialect: Default::default(),
            force_quoted_ident: false,
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: false,
            hide_options_in_show_create_table: true,
        };

        let mut mapping = HashMap::new();
        let mut statistics_tables = Vec::new();

        match &plan {
            Plan::Query { metadata, .. } => {
                let current_database = ctx.get_current_database();
                if !mapping.contains_key(&current_database) {
                    mapping.insert(current_database.clone(), visitor.unique_name());
                }

                let current_database = mapping.get(&current_database).unwrap().clone();
                self.replication_databases.insert(
                    current_database.clone(),
                    format!("CREATE DATABASE IF NOT EXISTS {};", current_database),
                );

                for table in metadata.read().tables() {
                    if !mapping.contains_key(table.catalog()) {
                        mapping.insert(table.catalog().to_string(), visitor.unique_name());
                    }

                    if !mapping.contains_key(table.database()) {
                        mapping.insert(table.database().to_string(), visitor.unique_name());
                    }

                    if !mapping.contains_key(table.name()) {
                        mapping.insert(table.name().to_string(), visitor.unique_name());
                    }

                    let mut table_info = table.table().get_table_info().clone();
                    let schema = table.table().schema();

                    if table_info.engine() == VIEW_ENGINE || table_info.engine() == STREAM_ENGINE {
                        return Err(ErrorCode::Unimplemented(
                            "Reports in SQL must not contain tables that use view or stream engines.",
                        ));
                    }

                    table_info.meta.comment = String::new();
                    table_info.meta.field_comments = vec!["".to_string(); schema.fields.len()];
                    table_info.name = mapping.get(table.name()).unwrap().clone();
                    table_info.desc = format!(
                        "{}.{}",
                        mapping.get(table.database()).unwrap(),
                        table_info.name
                    );

                    let mut table_schema = TableSchema::empty();

                    for field in schema.fields() {
                        if !mapping.contains_key(field.name()) {
                            mapping.insert(field.name().to_string(), visitor.unique_name());
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
            writeln!(f, "| Column | Ndv | Nc |")?;
            writeln!(f, "|--------|-----------------|------------|")?;

            for (col_name, stats) in &self.columns_statistics {
                writeln!(
                    f,
                    "| {} | {} | {} |",
                    col_name,
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

        if let Some(settings) = self.setting_changes.as_ref() {
            if settings.is_changed() {
                writeln!(f, "## Setting Changes")?;
                writeln!(f, "| Name         | Scope Level  | Change Value |")?;
                writeln!(f, "|--------------|--------------|--------------|")?;

                for item in settings.into_iter() {
                    if item.user_value != item.default_value {
                        writeln!(
                            f,
                            "|{} | {:?} | {} |",
                            item.name,
                            item.level,
                            item.user_value.as_string()
                        )?;
                    }
                }

                writeln!(f)?;
            }
        }

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
            writeln!(f, "```sql")?;
            for query in self.replication_queries.iter() {
                writeln!(f, "{};", query)?;
            }

            writeln!(f, "```")?;
        }

        if !self.logs.is_empty() {
            writeln!(f, "## Logs")?;
            writeln!(f, "```text")?;
            while let Ok(log) = self.logs.pop() {
                writeln!(f, "- {}", log)?;
            }
            writeln!(f, "```")?;
        }

        Ok(())
    }
}
