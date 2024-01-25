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

use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTaskStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub warehouse_opts: WarehouseOptions,
    pub schedule_opts: Option<ScheduleOptions>,
    pub session_parameters: BTreeMap<String, String>,
    pub suspend_task_after_num_failures: Option<u64>,
    pub comments: String,
    pub after: Vec<String>,
    pub when_condition: Option<String>,
    pub sql: String,
}

impl Display for CreateTaskStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TASK")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;

        write!(f, "{}", self.warehouse_opts)?;
        if let Some(schedule_opt) = self.schedule_opts.as_ref() {
            write!(f, "{}", schedule_opt)?;
        }

        if !self.session_parameters.is_empty() {
            for (key, value) in &self.session_parameters {
                write!(f, " {} = '{}'", key, value)?;
            }
        }

        if let Some(num) = self.suspend_task_after_num_failures {
            write!(f, " SUSPEND_TASK_AFTER {} FAILURES", num)?;
        }

        if !self.comments.is_empty() {
            write!(f, " COMMENTS = '{}'", self.comments)?;
        }

        if !self.after.is_empty() {
            write!(f, "AFTER = '{:?}'", self.after)?;
        }

        if self.when_condition.is_some() {
            write!(f, "WHEN = '{:?}'", self.when_condition)?;
        }

        write!(f, " AS {}", self.sql)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarehouseOptions {
    pub warehouse: Option<String>,
}

impl Display for WarehouseOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(wh) = &self.warehouse {
            write!(f, " WAREHOUSE = {}", wh)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleOptions {
    IntervalSecs(u64),
    CronExpression(String, Option<String>),
}

impl Display for ScheduleOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleOptions::IntervalSecs(secs) => {
                write!(f, " SCHEDULE {} SECOND", secs)
            }
            ScheduleOptions::CronExpression(expr, tz) => {
                write!(f, " SCHEDULE CRON '{}'", expr)?;
                if let Some(tz) = tz {
                    write!(f, " TIMEZONE '{}'", tz)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTaskStmt {
    pub if_exists: bool,
    pub name: String,
    pub options: AlterTaskOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTaskOptions {
    Resume,
    Suspend,
    Set {
        warehouse: Option<String>,
        schedule: Option<ScheduleOptions>,
        suspend_task_after_num_failures: Option<u64>,
        comments: Option<String>,
        session_parameters: Option<BTreeMap<String, String>>,
    },
    Unset {
        warehouse: bool,
    },
    // Change SQL
    ModifyAs(String),
    ModifyWhen(String),
    AddAfter(Vec<String>),
    RemoveAfter(Vec<String>),
}

impl Display for AlterTaskOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTaskOptions::Resume => write!(f, " RESUME"),
            AlterTaskOptions::Suspend => write!(f, " SUSPEND"),
            AlterTaskOptions::Set {
                warehouse,
                schedule,
                suspend_task_after_num_failures,
                session_parameters,
                comments,
            } => {
                if let Some(wh) = warehouse {
                    write!(f, " SET WAREHOUSE = {}", wh)?;
                }
                if let Some(schedule) = schedule {
                    write!(f, " SET {}", schedule)?;
                }
                if let Some(num) = suspend_task_after_num_failures {
                    write!(f, " SUSPEND TASK AFTER {} FAILURES", num)?;
                }
                if let Some(comments) = comments {
                    write!(f, " COMMENTS = '{}'", comments)?;
                }
                if let Some(session) = session_parameters {
                    for (key, value) in session {
                        write!(f, " {} = '{}'", key, value)?;
                    }
                }
                Ok(())
            }
            AlterTaskOptions::Unset { warehouse } => {
                if *warehouse {
                    write!(f, " UNSET WAREHOUSE")?;
                }
                Ok(())
            }
            AlterTaskOptions::ModifyAs(sql) => write!(f, " AS {}", sql),
            AlterTaskOptions::ModifyWhen(when) => write!(f, " WHEN {}", when),
            AlterTaskOptions::AddAfter(after) => write!(f, " ADD AFTER = '{:?}'", after),
            AlterTaskOptions::RemoveAfter(after) => write!(f, " REMOVE AFTER = '{:?}'", after),
        }
    }
}

impl Display for AlterTaskStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TASK")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        write!(f, "{}", self.options)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTaskStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropTaskStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TASK")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowTasksStmt {
    pub limit: Option<ShowLimit>,
}

impl Display for ShowTasksStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW ")?;
        write!(f, "TASKS")?;
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTaskStmt {
    pub name: String,
}

impl Display for ExecuteTaskStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXECUTE TASK {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTaskStmt {
    pub name: String,
}

impl Display for DescribeTaskStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DESCRIBE TASK {}", self.name)
    }
}
