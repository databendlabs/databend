// Copyright 2020 Datafuse Labs.
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
use common_meta_types::UserPrivilege;
use common_planners::GrantPrivilegePlan;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfGrantStatement {
    pub name: String,
    pub hostname: String,
    pub priv_types: UserPrivilege,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfGrantStatement {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::GrantPrivilege(
            GrantPrivilegePlan {
                name: self.name.clone(),
                hostname: self.hostname.clone(),
                priv_types: self.priv_types,
            },
        )))
    }
}
