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
use databend_common_settings::Settings;

use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::servers::flight::v1::actions::KillQuery;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub async fn kill_query(req: KillQuery) -> Result<()> {
    let config = GlobalConfig::instance();
    let session_manager = SessionManager::instance();
    let settings = Settings::create(config.query.tenant_id.clone());
    let session = session_manager.create_with_settings(SessionType::FlightRPC, settings)?;
    let ctx = session.create_query_context().await?;
    let interpreter = KillInterpreter::from_flight(ctx, req.packet)?;
    interpreter.execute2().await?;
    Ok(())
}
