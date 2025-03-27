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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_string_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::Expr;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TaskSql {
    SingleStatement(String),
    ScriptBlock(Vec<String>),
}

impl Display for TaskSql {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TaskSql::SingleStatement(stmt) => write!(f, "{}", stmt),
            TaskSql::ScriptBlock(stmts) => {
                writeln!(f, "BEGIN")?;
                for stmt in stmts {
                    writeln!(f, "{};", stmt)?;
                }
                write!(f, "END;")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateTaskOption {
    Warehouse(String),
    Schedule(ScheduleOptions),
    After(Vec<String>),
    When(Expr),
    SuspendTaskAfterNumFailures(u64),
    ErrorIntegration(String),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateTaskStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub warehouse: Option<String>,
    pub schedule_opts: Option<ScheduleOptions>,
    pub session_parameters: BTreeMap<String, String>,
    pub suspend_task_after_num_failures: Option<u64>,
    // notification_integration name for error
    pub error_integration: Option<String>,
    pub comments: Option<String>,
    pub after: Vec<String>,
    pub when_condition: Option<Expr>,
    pub sql: TaskSql,
}

impl CreateTaskStmt {
    pub fn apply_opt(&mut self, opt: CreateTaskOption) {
        match opt {
            CreateTaskOption::Warehouse(wh) => {
                self.warehouse = Some(wh);
            }
            CreateTaskOption::Schedule(schedule) => {
                self.schedule_opts = Some(schedule);
            }
            CreateTaskOption::After(after) => {
                self.after = after;
            }
            CreateTaskOption::When(when) => {
                self.when_condition = Some(when);
            }
            CreateTaskOption::SuspendTaskAfterNumFailures(num) => {
                self.suspend_task_after_num_failures = Some(num);
            }
            CreateTaskOption::ErrorIntegration(integration) => {
                self.error_integration = Some(integration);
            }
            CreateTaskOption::Comment(comment) => {
                self.comments = Some(comment);
            }
        }
    }
}

impl Display for CreateTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE TASK")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }

        write!(f, " {}", self.name)?;

        if let Some(warehouse) = self.warehouse.as_ref() {
            write!(f, " WAREHOUSE = '{}'", warehouse)?;
        }

        if let Some(schedule_opt) = self.schedule_opts.as_ref() {
            write!(f, " SCHEDULE = {}", schedule_opt)?;
        }

        if let Some(num) = self.suspend_task_after_num_failures {
            write!(f, " SUSPEND_TASK_AFTER_NUM_FAILURES = {}", num)?;
        }

        if !self.after.is_empty() {
            write!(f, " AFTER ")?;
            write_comma_separated_string_list(f, &self.after)?;
        }

        if let Some(when_condition) = &self.when_condition {
            write!(f, " WHEN {}", when_condition)?;
        }
        if let Some(error_integration) = &self.error_integration {
            write!(f, " ERROR_INTEGRATION = '{}'", error_integration)?;
        }

        if let Some(comments) = &self.comments {
            write!(f, " COMMENTS = '{}'", comments)?;
        }

        if !self.session_parameters.is_empty() {
            write!(f, " ")?;
            write_comma_separated_string_map(f, &self.session_parameters)?;
        }

        write!(f, " AS {}", self.sql)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WarehouseOptions {
    pub warehouse: Option<String>,
}

impl Display for WarehouseOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(wh) = &self.warehouse {
            write!(f, "WAREHOUSE = '{}'", wh)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum ScheduleOptions {
    IntervalSecs(u64, u64),
    CronExpression(String, Option<String>),
}

impl Display for ScheduleOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScheduleOptions::IntervalSecs(secs, ms) => {
                if *ms > 0 {
                    write!(f, "{} MILLISECOND", ms)?;
                    Ok(())
                } else {
                    write!(f, "{} SECOND", secs)?;
                    Ok(())
                }
            }
            ScheduleOptions::CronExpression(expr, tz) => {
                write!(f, "USING CRON '{}'", expr)?;
                if let Some(tz) = tz {
                    write!(f, " '{}'", tz)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterTaskStmt {
    pub if_exists: bool,
    pub name: String,
    pub options: AlterTaskOptions,
}

impl Display for AlterTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER TASK")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        write!(f, " {}", self.options)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTaskSetOption {
    Warehouse(String),
    Schedule(ScheduleOptions),
    SuspendTaskAfterNumFailures(u64),
    ErrorIntegration(String),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterTaskOptions {
    Resume,
    Suspend,
    Set {
        warehouse: Option<String>,
        schedule: Option<ScheduleOptions>,
        suspend_task_after_num_failures: Option<u64>,
        comments: Option<String>,
        session_parameters: Option<BTreeMap<String, String>>,
        error_integration: Option<String>,
    },
    Unset {
        warehouse: bool,
    },
    // Change SQL
    ModifyAs(TaskSql),
    ModifyWhen(Expr),
    AddAfter(Vec<String>),
    RemoveAfter(Vec<String>),
}

impl AlterTaskOptions {
    pub fn apply_opt(&mut self, opt: AlterTaskSetOption) {
        if let AlterTaskOptions::Set {
            warehouse,
            schedule,
            suspend_task_after_num_failures,
            session_parameters: _,
            error_integration,
            comments,
        } = self
        {
            match opt {
                AlterTaskSetOption::Warehouse(wh) => {
                    *warehouse = Some(wh);
                }
                AlterTaskSetOption::Schedule(s) => {
                    *schedule = Some(s);
                }
                AlterTaskSetOption::ErrorIntegration(integration) => {
                    *error_integration = Some(integration);
                }
                AlterTaskSetOption::SuspendTaskAfterNumFailures(num) => {
                    *suspend_task_after_num_failures = Some(num);
                }
                AlterTaskSetOption::Comment(comment) => {
                    *comments = Some(comment);
                }
            }
        }
    }
}

impl Display for AlterTaskOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterTaskOptions::Resume => write!(f, "RESUME"),
            AlterTaskOptions::Suspend => write!(f, "SUSPEND"),
            AlterTaskOptions::Set {
                warehouse,
                schedule,
                suspend_task_after_num_failures,
                session_parameters,
                error_integration,
                comments,
            } => {
                write!(f, "SET")?;
                if let Some(wh) = warehouse {
                    write!(f, " WAREHOUSE = '{wh}'")?;
                }
                if let Some(schedule) = schedule {
                    write!(f, " SCHEDULE = {schedule}")?;
                }
                if let Some(num) = suspend_task_after_num_failures {
                    write!(f, " SUSPEND_TASK_AFTER_NUM_FAILURES = {num}")?;
                }
                if let Some(comments) = comments {
                    write!(f, " COMMENT = {}", QuotedString(comments, '\''))?;
                }
                if let Some(error_integration) = error_integration {
                    write!(f, " ERROR_INTEGRATION = '{error_integration}'")?;
                }
                if let Some(session) = session_parameters {
                    write!(f, " ")?;
                    write_comma_separated_string_map(f, session)?;
                }
                Ok(())
            }
            AlterTaskOptions::Unset { warehouse } => {
                if *warehouse {
                    write!(f, "UNSET WAREHOUSE")?;
                }
                Ok(())
            }
            AlterTaskOptions::ModifyAs(sql) => write!(f, "MODIFY AS {sql}"),
            AlterTaskOptions::ModifyWhen(expr) => write!(f, "MODIFY WHEN {expr}"),
            AlterTaskOptions::AddAfter(after) => {
                write!(f, "ADD AFTER ")?;
                write_comma_separated_string_list(f, after)
            }
            AlterTaskOptions::RemoveAfter(after) => {
                write!(f, "REMOVE AFTER ")?;
                write_comma_separated_string_list(f, after)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropTaskStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP TASK")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowTasksStmt {
    pub limit: Option<ShowLimit>,
}

impl Display for ShowTasksStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW ")?;
        write!(f, "TASKS")?;
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ExecuteTaskStmt {
    pub name: String,
}

impl Display for ExecuteTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EXECUTE TASK {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescribeTaskStmt {
    pub name: String,
}

impl Display for DescribeTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE TASK {}", self.name)
    }
}
