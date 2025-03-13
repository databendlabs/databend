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
use std::sync::Arc;

use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::ScheduleOptions;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::TaskSql;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType::Int32;
use databend_common_expression::types::NumberDataType::Int64;
use databend_common_expression::types::NumberDataType::UInt64;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::tenant::Tenant;

pub fn task_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("created_on", DataType::Timestamp),
        DataField::new("name", DataType::String),
        DataField::new("id", DataType::Number(UInt64)),
        DataField::new("owner", DataType::String),
        DataField::new("comment", DataType::String.wrap_nullable()),
        DataField::new("warehouse", DataType::String.wrap_nullable()),
        DataField::new("schedule", DataType::String.wrap_nullable()),
        DataField::new("state", DataType::String),
        DataField::new("definition", DataType::String),
        DataField::new("condition_text", DataType::String),
        DataField::new("after", DataType::String),
        DataField::new(
            "suspend_task_after_num_failures",
            DataType::Number(UInt64).wrap_nullable(),
        ),
        DataField::new("error_integration", DataType::String.wrap_nullable()),
        DataField::new("next_schedule_time", DataType::Timestamp.wrap_nullable()),
        DataField::new("last_committed_on", DataType::Timestamp),
        DataField::new("last_suspended_on", DataType::Timestamp.wrap_nullable()),
        DataField::new("session_parameters", DataType::Variant.wrap_nullable()),
    ]))
}

pub fn task_run_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("name", DataType::String),
        DataField::new("id", DataType::Number(UInt64)),
        DataField::new("owner", DataType::String),
        DataField::new("comment", DataType::String.wrap_nullable()),
        DataField::new("schedule", DataType::String.wrap_nullable()),
        DataField::new("warehouse", DataType::String.wrap_nullable()),
        DataField::new("state", DataType::String),
        DataField::new("definition", DataType::String),
        DataField::new("condition_text", DataType::String),
        DataField::new("run_id", DataType::String),
        DataField::new("query_id", DataType::String),
        DataField::new("exception_code", DataType::Number(Int64)),
        DataField::new("exception_text", DataType::String.wrap_nullable()),
        DataField::new("attempt_number", DataType::Number(Int32)),
        DataField::new("completed_time", DataType::Timestamp.wrap_nullable()),
        DataField::new("scheduled_time", DataType::Timestamp),
        DataField::new("root_task_id", DataType::String),
        DataField::new("session_parameters", DataType::Variant.wrap_nullable()),
    ]))
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTaskPlan {
    pub if_not_exists: bool,
    pub tenant: Tenant,
    pub task_name: String,
    pub warehouse: Option<String>,
    pub schedule_opts: Option<ScheduleOptions>,
    pub after: Vec<String>,
    pub when_condition: Option<String>,
    pub suspend_task_after_num_failures: Option<u64>,
    pub error_integration: Option<String>,
    pub session_parameters: BTreeMap<String, String>,
    pub sql: TaskSql,
    pub comment: Option<String>,
}

impl CreateTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterTaskPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub task_name: String,
    pub alter_options: AlterTaskOptions,
}

impl AlterTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropTaskPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub task_name: String,
}

impl DropTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeTaskPlan {
    pub tenant: Tenant,
    pub task_name: String,
}

impl DescribeTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        task_schema()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteTaskPlan {
    pub tenant: Tenant,
    pub task_name: String,
}

impl ExecuteTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShowTasksPlan {
    pub tenant: Tenant,
    pub limit: Option<ShowLimit>,
}

impl ShowTasksPlan {
    pub fn schema(&self) -> DataSchemaRef {
        task_schema()
    }
}
