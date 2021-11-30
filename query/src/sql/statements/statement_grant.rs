// Copyright 2021 Datafuse Labs.
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
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilege;
use common_planners::GrantPrivilegePlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfGrantStatement {
    pub name: String,
    pub hostname: String,
    pub priv_types: UserPrivilege,
    pub on: DfGrantObject,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DfGrantObject {
    Global,
    Database(Option<String>),
    Table(Option<String>, String),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfGrantStatement {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::GrantPrivilege(GrantPrivilegePlan {
                name: self.name.clone(),
                hostname: self.hostname.clone(),
                on: match &self.on {
                    DfGrantObject::Global => GrantObject::Global,
                    DfGrantObject::Table(database_name, table_name) => {
                        let database_name = database_name
                            .clone()
                            .unwrap_or_else(|| ctx.get_current_database());
                        GrantObject::Table(database_name, table_name.clone())
                    }
                    DfGrantObject::Database(database_name) => {
                        let database_name = database_name
                            .clone()
                            .unwrap_or_else(|| ctx.get_current_database());
                        GrantObject::Database(database_name)
                    }
                },
                priv_types: self.priv_types,
            }),
        )))
    }
}
