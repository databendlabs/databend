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

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_sql::plans::SystemPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::SystemActionInterpreter;
use crate::servers::flight::v1::actions::create_session;

pub static SYSTEM_ACTION: &str = "/actions/system_action";

pub async fn system_action(plan: SystemPlan) -> Result<()> {
    let session = create_session()?;
    let version = GlobalConfig::version();
    let query_context = session.create_query_context(version).await?;
    let interpreter = SystemActionInterpreter::from_flight(query_context, plan)?;
    interpreter.execute2().await.map(|_| ())
}
