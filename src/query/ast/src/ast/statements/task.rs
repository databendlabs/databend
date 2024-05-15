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

use crate::ast::write_comma_separated_string_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::Expr;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TaskSql {
    SingleStatement(#[drive(skip)] String),
    ScriptBlock(#[drive(skip)] Vec<String>),
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

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateTaskStmt {
    #[drive(skip)]
    pub if_not_exists: bool,
    #[drive(skip)]
    pub name: String,
    pub warehouse_opts: WarehouseOptions,
    pub schedule_opts: Option<ScheduleOptions>,
    #[drive(skip)]
    pub session_parameters: BTreeMap<String, String>,
    #[drive(skip)]
    pub suspend_task_after_num_failures: Option<u64>,
    // notification_integration name for error
    #[drive(skip)]
    pub error_integration: Option<String>,
    #[drive(skip)]
    pub comments: Option<String>,
    #[drive(skip)]
    pub after: Vec<String>,
    #[drive(skip)]
    pub when_condition: Option<Expr>,
    #[drive(skip)]
    pub sql: TaskSql,
}

impl Display for CreateTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE TASK")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }

        write!(f, " {}", self.name)?;

        if self.warehouse_opts.warehouse.is_some() {
            write!(f, " {}", self.warehouse_opts)?;
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
    #[drive(skip)]
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
    IntervalSecs(#[drive(skip)] u64),
    CronExpression(#[drive(skip)] String, #[drive(skip)] Option<String>),
}

impl Display for ScheduleOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScheduleOptions::IntervalSecs(secs) => {
                write!(f, "{} SECOND", secs)
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
    #[drive(skip)]
    pub if_exists: bool,
    #[drive(skip)]
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

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterTaskOptions {
    Resume,
    Suspend,
    Set {
        #[drive(skip)]
        warehouse: Option<String>,
        schedule: Option<ScheduleOptions>,
        #[drive(skip)]
        suspend_task_after_num_failures: Option<u64>,
        #[drive(skip)]
        comments: Option<String>,
        #[drive(skip)]
        session_parameters: Option<BTreeMap<String, String>>,
        #[drive(skip)]
        error_integration: Option<String>,
    },
    Unset {
        #[drive(skip)]
        warehouse: bool,
    },
    // Change SQL
    ModifyAs(#[drive(skip)] TaskSql),
    ModifyWhen(Expr),
    AddAfter(#[drive(skip)] Vec<String>),
    RemoveAfter(#[drive(skip)] Vec<String>),
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
                    write!(f, " COMMENT = '{comments}'")?;
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
    #[drive(skip)]
    pub if_exists: bool,
    #[drive(skip)]
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
    #[drive(skip)]
    pub name: String,
}

impl Display for ExecuteTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EXECUTE TASK {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescribeTaskStmt {
    #[drive(skip)]
    pub name: String,
}

impl Display for DescribeTaskStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE TASK {}", self.name)
    }
}
