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

use std::sync::Arc;

use common_exception::Result;
use common_legacy_planners::PlanNode;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    async fn check(&self, plan: &PlanNode) -> Result<()> {
        match plan {
            PlanNode::Empty(_) => {}
            PlanNode::Stage(_) => {}
            PlanNode::Broadcast(_) => {}
            PlanNode::Remote(_) => {}
            PlanNode::Projection(_) => {}
            PlanNode::Expression(_) => {}
            PlanNode::AggregatorPartial(_) => {}
            PlanNode::AggregatorFinal(_) => {}
            PlanNode::Filter(_) => {}
            PlanNode::Having(_) => {}
            PlanNode::WindowFunc(_) => {}
            PlanNode::Sort(_) => {}
            PlanNode::Limit(_) => {}
            PlanNode::LimitBy(_) => {}
            PlanNode::ReadSource(_) => {}
            PlanNode::SubQueryExpression(_) => {}
            PlanNode::Sink(_) => {}
            PlanNode::Explain(_) => {}
            PlanNode::Select(_) => {}
            PlanNode::Insert(_) => {}
            PlanNode::Delete(_) => {}
        }
        Ok(())
    }

    async fn check_new(&self, plan: &Plan) -> Result<()> {
        match plan {
            Plan::Query { .. } => {}
            Plan::Explain { .. } => {}
            Plan::Copy(_) => {}
            Plan::Call(_) => {}
            Plan::ShowCreateDatabase(_) => {}
            Plan::CreateDatabase(_) => {}
            Plan::DropDatabase(_) => {}
            Plan::UndropDatabase(_) => {}
            Plan::RenameDatabase(_) => {}
            Plan::UseDatabase(_) => {}
            Plan::ShowCreateTable(_) => {}
            Plan::DescribeTable(_) => {}
            Plan::CreateTable(_) => {}
            Plan::DropTable(_) => {}
            Plan::UndropTable(_) => {}
            Plan::RenameTable(_) => {}
            Plan::AlterTableClusterKey(_) => {}
            Plan::DropTableClusterKey(_) => {}
            Plan::ReclusterTable(_) => {}
            Plan::TruncateTable(_) => {}
            Plan::OptimizeTable(_) => {}
            Plan::ExistsTable(_) => {}
            Plan::Insert(_) => {}
            Plan::Delete(_) => {}
            Plan::CreateView(_) => {}
            Plan::AlterView(_) => {}
            Plan::DropView(plan) => {
                self.ctx
                    .get_current_session()
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Drop,
                    )
                    .await?;
            }
            Plan::AlterUser(_) => {}
            Plan::CreateUser(_) => {}
            Plan::DropUser(_) => {}
            Plan::CreateUDF(_) => {}
            Plan::AlterUDF(_) => {}
            Plan::DropUDF(_) => {}
            Plan::CreateRole(_) => {}
            Plan::DropRole(_) => {}
            Plan::GrantRole(_) => {}
            Plan::GrantPriv(_) => {}
            Plan::ShowGrants(_) => {}
            Plan::RevokePriv(_) => {}
            Plan::RevokeRole(_) => {}
            Plan::ListStage(_) => {}
            Plan::CreateStage(_) => {}
            Plan::DropStage(_) => {}
            Plan::RemoveStage(_) => {}
            Plan::Presign(_) => {}
            Plan::SetVariable(_) => {}
            Plan::Kill(_) => {}
            Plan::CreateShare(_) => {}
            Plan::DropShare(_) => {}
            Plan::GrantShareObject(_) => {}
            Plan::RevokeShareObject(_) => {}
            Plan::AlterShareTenants(_) => {}
            Plan::DescShare(_) => {}
            Plan::ShowShares(_) => {}
            Plan::ShowObjectGrantPrivileges(_) => {}
            Plan::ShowGrantTenantsOfShare(_) => {}
        }

        Ok(())
    }
}
