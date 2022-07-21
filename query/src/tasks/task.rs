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

use super::compaction::CompactionTask;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::Config;

pub struct Task {
    sessions: Arc<SessionManager>,
    conf: Config,
}

impl Task {
    pub fn new(sessions: Arc<SessionManager>, conf: Config) -> Self {
        Self { sessions, conf }
    }

    pub async fn start(&self) -> Result<()> {
        if self.conf.task.compaction_enabled {
            let executor_session = self.sessions.create_session(SessionType::Dummy).await?;
            let ctx = executor_session.create_query_context().await?;
            let compaction_task = CompactionTask::try_create(self.conf.clone(), ctx)?;
            compaction_task.start().await?;
        }
        Ok(())
    }
}
