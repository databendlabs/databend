// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use crate::sql::plans::Plan;

impl Plan {
    pub fn format_indent(&self) -> Result<String> {
        match self {
            Plan::Query {
                s_expr, metadata, ..
            } => s_expr.to_format_tree(metadata).format_indent(),
            Plan::Explain { kind, plan } => {
                let result = plan.format_indent()?;
                Ok(format!("{:?}:\n{}", kind, result))
            }

            Plan::ShowMetrics => Ok("SHOW METRICS".to_string()),
            Plan::ShowProcessList => Ok("SHOW PROCESSLIST".to_string()),
            Plan::ShowSettings => Ok("SHOW SETTINGS".to_string()),

            // Databases
            Plan::CreateDatabase(create_database) => Ok(format!("{:?}", create_database)),
            Plan::DropDatabase(drop_database) => Ok(format!("{:?}", drop_database)),
            Plan::RenameDatabase(rename_database) => Ok(format!("{:?}", rename_database)),

            // Tables
            Plan::CreateTable(create_table) => Ok(format!("{:?}", create_table)),

            // Views
            Plan::CreateView(create_view) => Ok(format!("{:?}", create_view)),
            Plan::AlterView(alter_view) => Ok(format!("{:?}", alter_view)),

            // Users
            Plan::CreateUser(create_user) => Ok(format!("{:?}", create_user)),
            Plan::DropUser(drop_user) => Ok(format!("{:?}", drop_user)),
            Plan::AlterUser(alter_user) => Ok(format!("{:?}", alter_user)),

            // Stages
            Plan::ShowStages => Ok("SHOW STAGES".to_string()),
            Plan::ListStage(s) => Ok(format!("{:?}", s)),
            Plan::DescribeStage(s) => Ok(format!("{:?}", s)),
            Plan::CreateStage(create_stage) => Ok(format!("{:?}", create_stage)),
            Plan::DropStage(s) => Ok(format!("{:?}", s)),
            Plan::RemoveStage(s) => Ok(format!("{:?}", s)),
        }
    }
}
