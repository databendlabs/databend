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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_legacy_planners::PlanNode;

use crate::interpreters::ManagementModeAccess;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub trait AccessChecker: Sync + Send {
    // Check the access permission for the old plan.
    // TODO(bohu): Remove after new plan done.
    fn check(&self, plan: &PlanNode) -> Result<()>;

    // Check the access permission for the old plan.
    fn check_new(&self, _plan: &Plan) -> Result<()>;
}

pub struct Accessor {
    accessors: HashMap<String, Box<dyn AccessChecker>>,
}

impl Accessor {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        let mut accessors: HashMap<String, Box<dyn AccessChecker>> = Default::default();
        accessors.insert("management".to_string(), ManagementModeAccess::create(ctx));
        Accessor { accessors }
    }

    pub fn check(&self, new_plan: &Option<Plan>, plan: &PlanNode) -> Result<()> {
        for accessor in self.accessors.values() {
            match new_plan {
                None => {
                    accessor.check(plan)?;
                }
                Some(new) => {
                    accessor.check_new(new)?;
                }
            }
        }
        Ok(())
    }
}
