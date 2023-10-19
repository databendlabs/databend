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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::ROW_ID_COL_NAME;

use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::DeletePlan;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::ScalarExpr;
use crate::Visibility;

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
            Plan::ExplainAst { .. } => Ok("ExplainAst".to_string()),
            Plan::ExplainSyntax { .. } => Ok("ExplainSyntax".to_string()),
            Plan::ExplainAnalyze { .. } => Ok("ExplainAnalyze".to_string()),

            Plan::CopyIntoTable(plan) => Ok(format!("{:?}", plan)),
            Plan::CopyIntoLocation(plan) => Ok(format!("{:?}", plan)),

            // catalog
            Plan::ShowCreateCatalog(show_create_catalog) => {
                Ok(format!("{:?}", show_create_catalog))
            }
            Plan::CreateCatalog(create_catalog) => Ok(format!("{:?}", create_catalog)),
            Plan::DropCatalog(drop_catalog) => Ok(format!("{:?}", drop_catalog)),

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
            Plan::SetOptions(set_options) => Ok(format!("{:?}", set_options)),
            Plan::RenameTableColumn(rename_table_column) => {
                Ok(format!("{:?}", rename_table_column))
            }
            Plan::AddTableColumn(add_table_column) => Ok(format!("{:?}", add_table_column)),
            Plan::ModifyTableColumn(modify_table_column) => {
                Ok(format!("{:?}", modify_table_column))
            }
            Plan::DropTableColumn(drop_table_column) => Ok(format!("{:?}", drop_table_column)),
            Plan::AlterTableClusterKey(alter_table_cluster_key) => {
                Ok(format!("{:?}", alter_table_cluster_key))
            }
            Plan::DropTableClusterKey(drop_table_cluster_key) => {
                Ok(format!("{:?}", drop_table_cluster_key))
            }
            Plan::ReclusterTable(recluster_table) => Ok(format!("{:?}", recluster_table)),
            Plan::TruncateTable(truncate_table) => Ok(format!("{:?}", truncate_table)),
            Plan::OptimizeTable(optimize_table) => Ok(format!("{:?}", optimize_table)),
            Plan::VacuumTable(vacuum_table) => Ok(format!("{:?}", vacuum_table)),
            Plan::VacuumDropTable(vacuum_drop_table) => Ok(format!("{:?}", vacuum_drop_table)),
            Plan::AnalyzeTable(analyze_table) => Ok(format!("{:?}", analyze_table)),
            Plan::ExistsTable(exists_table) => Ok(format!("{:?}", exists_table)),

            // Views
            Plan::CreateView(create_view) => Ok(format!("{:?}", create_view)),
            Plan::AlterView(alter_view) => Ok(format!("{:?}", alter_view)),
            Plan::DropView(drop_view) => Ok(format!("{:?}", drop_view)),

            // Indexes
            Plan::CreateIndex(index) => Ok(format!("{:?}", index)),
            Plan::DropIndex(index) => Ok(format!("{:?}", index)),
            Plan::RefreshIndex(index) => Ok(format!("{index:?}")),

            // Virtual Columns
            Plan::CreateVirtualColumn(create_virtual_column) => {
                Ok(format!("{:?}", create_virtual_column))
            }
            Plan::AlterVirtualColumn(alter_virtual_column) => {
                Ok(format!("{:?}", alter_virtual_column))
            }
            Plan::DropVirtualColumn(drop_virtual_column) => {
                Ok(format!("{:?}", drop_virtual_column))
            }
            Plan::RefreshVirtualColumn(refresh_virtual_column) => {
                Ok(format!("{:?}", refresh_virtual_column))
            }

            // Insert
            Plan::Insert(insert) => Ok(format!("{:?}", insert)),
            Plan::Replace(replace) => Ok(format!("{:?}", replace)),
            Plan::MergeInto(merge_into) => Ok(format!("{:?}", merge_into)),
            Plan::Delete(delete) => format_delete(delete),
            Plan::Update(update) => Ok(format!("{:?}", update)),

            // Stages
            Plan::CreateStage(create_stage) => Ok(format!("{:?}", create_stage)),
            Plan::DropStage(s) => Ok(format!("{:?}", s)),
            Plan::RemoveStage(s) => Ok(format!("{:?}", s)),

            // FileFormat
            Plan::CreateFileFormat(create_file_format) => Ok(format!("{:?}", create_file_format)),
            Plan::DropFileFormat(drop_file_format) => Ok(format!("{:?}", drop_file_format)),
            Plan::ShowFileFormats(show_file_formats) => Ok(format!("{:?}", show_file_formats)),

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
            Plan::UnSetVariable(p) => Ok(format!("{:?}", p)),
            Plan::SetRole(p) => Ok(format!("{:?}", p)),
            Plan::UseDatabase(p) => Ok(format!("{:?}", p)),
            Plan::Kill(p) => Ok(format!("{:?}", p)),

            Plan::CreateShareEndpoint(p) => Ok(format!("{:?}", p)),
            Plan::ShowShareEndpoint(p) => Ok(format!("{:?}", p)),
            Plan::DropShareEndpoint(p) => Ok(format!("{:?}", p)),
            Plan::CreateShare(p) => Ok(format!("{:?}", p)),
            Plan::DropShare(p) => Ok(format!("{:?}", p)),
            Plan::GrantShareObject(p) => Ok(format!("{:?}", p)),
            Plan::RevokeShareObject(p) => Ok(format!("{:?}", p)),
            Plan::AlterShareTenants(p) => Ok(format!("{:?}", p)),
            Plan::DescShare(p) => Ok(format!("{:?}", p)),
            Plan::ShowShares(p) => Ok(format!("{:?}", p)),
            Plan::ShowRoles(p) => Ok(format!("{:?}", p)),
            Plan::ShowObjectGrantPrivileges(p) => Ok(format!("{:?}", p)),
            Plan::ShowGrantTenantsOfShare(p) => Ok(format!("{:?}", p)),
            Plan::RevertTable(p) => Ok(format!("{:?}", p)),

            // data mask
            Plan::CreateDatamaskPolicy(p) => Ok(format!("{:?}", p)),
            Plan::DropDatamaskPolicy(p) => Ok(format!("{:?}", p)),
            Plan::DescDatamaskPolicy(p) => Ok(format!("{:?}", p)),

            // network policy
            Plan::CreateNetworkPolicy(p) => Ok(format!("{:?}", p)),
            Plan::AlterNetworkPolicy(p) => Ok(format!("{:?}", p)),
            Plan::DropNetworkPolicy(p) => Ok(format!("{:?}", p)),
            Plan::DescNetworkPolicy(p) => Ok(format!("{:?}", p)),
            Plan::ShowNetworkPolicies(p) => Ok(format!("{:?}", p)),

            // task
            Plan::CreateTask(p) => Ok(format!("{:?}", p)),
        }
    }
}

fn format_delete(delete: &DeletePlan) -> Result<String> {
    let table_index = delete
        .metadata
        .read()
        .get_table_index(
            Some(delete.database_name.as_str()),
            delete.table_name.as_str(),
        )
        .unwrap();
    let s_expr = if !delete.subquery_desc.is_empty() {
        let row_id_column_binding = ColumnBindingBuilder::new(
            ROW_ID_COL_NAME.to_string(),
            delete.subquery_desc[0].index,
            Box::new(DataType::Number(NumberDataType::UInt64)),
            Visibility::InVisible,
        )
        .database_name(Some(delete.database_name.clone()))
        .table_name(Some(delete.table_name.clone()))
        .table_index(Some(table_index))
        .build();
        SExpr::create_unary(
            Arc::new(RelOperator::EvalScalar(EvalScalar {
                items: vec![ScalarItem {
                    scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: row_id_column_binding,
                    }),
                    index: 0,
                }],
            })),
            Arc::new(delete.subquery_desc[0].input_expr.clone()),
        )
    } else {
        let scan = RelOperator::Scan(Scan {
            table_index,
            columns: Default::default(),
            push_down_predicates: None,
            limit: None,
            order_by: None,
            prewhere: None,
            agg_index: None,
            statistics: Default::default(),
        });
        let scan_expr = SExpr::create_leaf(Arc::new(scan));
        let mut predicates = vec![];
        if let Some(selection) = &delete.selection {
            predicates.push(selection.clone());
        }
        let filter = RelOperator::Filter(Filter { predicates });
        SExpr::create_unary(Arc::new(filter), Arc::new(scan_expr))
    };
    let res = s_expr.to_format_tree(&delete.metadata).format_pretty()?;
    Ok(format!("DeletePlan:\n{res}"))
}
