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
use common_sql::plans::SetOptionsPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetOptionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetOptionsPlan,
}

impl SetOptionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetOptionsPlan) -> Result<Self> {
        Ok(SetOptionsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetOptionsInterpreter {
    fn name(&self) -> &str {
        "SetOptionsInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // todo!!!
        // valid_options_check and do request to meta_srv
        Ok(PipelineBuildResult::create())
    }
}
