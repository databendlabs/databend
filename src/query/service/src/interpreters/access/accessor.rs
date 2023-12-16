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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::interpreters::access::PrivilegeAccess;
use crate::interpreters::ManagementModeAccess;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

#[async_trait::async_trait]
pub trait AccessChecker: Sync + Send {
    // Check the access permission for the plan.
    async fn check(&self, ctx: &Arc<QueryContext>, _plan: &Plan) -> Result<()>;
}

pub struct Accessor {
    ctx: Arc<QueryContext>,
    accessors: HashMap<String, Box<dyn AccessChecker>>,
}

impl Accessor {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        let mut accessors: HashMap<String, Box<dyn AccessChecker>> = Default::default();
        accessors.insert("management".to_string(), ManagementModeAccess::create());
        accessors.insert(
            "privilege".to_string(),
            PrivilegeAccess::create(ctx.clone()),
        );
        Accessor { ctx, accessors }
    }

    #[async_backtrace::framed]
    pub async fn check(&self, plan: &Plan) -> Result<()> {
        for accessor in self.accessors.values() {
            accessor.check(&self.ctx, plan).await?;
        }
        Ok(())
    }
}
