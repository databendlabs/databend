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
            } => s_expr.to_format_tree(metadata).format_pretty(),
            Plan::Explain { kind, plan } => {
                let result = plan.format_indent()?;
                Ok(format!("{:?}:\n{}", kind, result))
            }

            Plan::Copy(plan) => Ok(format!("{:?}", plan)),

            Plan::Call(plan) => Ok(format!("{:?}", plan)),

            // Databases
            Plan::ShowCreateDatabase(show_create_database) => {
                Ok(format!("{:?}", show_create_database))
            }
            Plan::CreateDatabase(create_database) => Ok(format!("{:?}", create_database)),
            Plan::DropDatabase(drop_database) => Ok(format!("{:?}", drop_database)),
            Plan::UndropDatabase(undrop_database) => Ok(format!("{:?}", undrop_database)),
            Plan::RenameDatabase(rename_database) => Ok(format!("{:?}", rename_database)),

            // Tables
            Plan::ShowCreateTable(show_create_table) => Ok(format!("{:?}", show_create_table)),
            Plan::CreateTable(create_table) => Ok(format!("{:?}", create_table)),
            Plan::DropTable(drop_table) => Ok(format!("{:?}", drop_table)),
            Plan::UndropTable(undrop_table) => Ok(format!("{:?}", undrop_table)),
            Plan::DescribeTable(describe_table) => Ok(format!("{:?}", describe_table)),
            Plan::RenameTable(rename_table) => Ok(format!("{:?}", rename_table)),
            Plan::AlterTableClusterKey(alter_table_cluster_key) => {
                Ok(format!("{:?}", alter_table_cluster_key))
            }
            Plan::DropTableClusterKey(drop_table_cluster_key) => {
                Ok(format!("{:?}", drop_table_cluster_key))
            }
            Plan::TruncateTable(truncate_table) => Ok(format!("{:?}", truncate_table)),
            Plan::OptimizeTable(optimize_table) => Ok(format!("{:?}", optimize_table)),
            Plan::ExistsTable(exists_table) => Ok(format!("{:?}", exists_table)),

            // Views
            Plan::CreateView(create_view) => Ok(format!("{:?}", create_view)),
            Plan::AlterView(alter_view) => Ok(format!("{:?}", alter_view)),
            Plan::DropView(drop_view) => Ok(format!("{:?}", drop_view)),

            // Insert
            Plan::Insert(insert) => Ok(format!("{:?}", insert)),
            Plan::Delete(delete) => Ok(format!("{:?}", delete)),

            // Stages
            Plan::ListStage(s) => Ok(format!("{:?}", s)),
            Plan::CreateStage(create_stage) => Ok(format!("{:?}", create_stage)),
            Plan::DropStage(s) => Ok(format!("{:?}", s)),
            Plan::RemoveStage(s) => Ok(format!("{:?}", s)),

            // Account
            Plan::GrantRole(grant_role) => Ok(format!("{:?}", grant_role)),
            Plan::GrantPriv(grant_priv) => Ok(format!("{:?}", grant_priv)),
            Plan::ShowGrants(show_grants) => Ok(format!("{:?}", show_grants)),
            Plan::RevokePriv(revoke_priv) => Ok(format!("{:?}", revoke_priv)),
            Plan::RevokeRole(revoke_role) => Ok(format!("{:?}", revoke_role)),
            Plan::CreateUser(create_user) => Ok(format!("{:?}", create_user)),
            Plan::DropUser(drop_user) => Ok(format!("{:?}", drop_user)),
            Plan::CreateUDF(create_user_udf) => Ok(format!("{:?}", create_user_udf)),
            Plan::AlterUDF(alter_user_udf) => Ok(format!("{alter_user_udf:?}")),
            Plan::DropUDF(drop_udf) => Ok(format!("{drop_udf:?}")),
            Plan::AlterUser(alter_user) => Ok(format!("{:?}", alter_user)),
            Plan::CreateRole(create_role) => Ok(format!("{:?}", create_role)),
            Plan::DropRole(drop_role) => Ok(format!("{:?}", drop_role)),

            Plan::Presign(presign) => Ok(format!("{:?}", presign)),

            Plan::SetVariable(p) => Ok(format!("{:?}", p)),
            Plan::UseDatabase(p) => Ok(format!("{:?}", p)),
            Plan::Kill(p) => Ok(format!("{:?}", p)),

            Plan::CreateShare(p) => Ok(format!("{:?}", p)),
            Plan::DropShare(p) => Ok(format!("{:?}", p)),
            Plan::GrantShareObject(p) => Ok(format!("{:?}", p)),
            Plan::RevokeShareObject(p) => Ok(format!("{:?}", p)),
            Plan::AlterShareTenants(p) => Ok(format!("{:?}", p)),
            Plan::DescShare(p) => Ok(format!("{:?}", p)),
            Plan::ShowShares(p) => Ok(format!("{:?}", p)),
        }
    }
}
