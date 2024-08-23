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

use databend_common_exception::Result;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_session::TxnManagerRef;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
pub struct AbortInterpreter {
    txn_manager: TxnManagerRef,
}

impl AbortInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(Self {
            txn_manager: ctx.txn_mgr(),
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for AbortInterpreter {
    fn name(&self) -> &str {
        "AbortInterpreter"
    }

    fn is_txn_command(&self) -> bool {
        true
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        self.txn_manager.lock().clear();
        Ok(PipelineBuildResult::create())
    }
}
