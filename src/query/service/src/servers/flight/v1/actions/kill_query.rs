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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_sql::plans::KillPlan;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::interpreters::KillQueryAccess;
use crate::servers::flight::v1::actions::create_session;

pub static KILL_QUERY: &str = "/actions/kill_query";
pub static KILL_QUERY_V2: &str = "/actions/kill_query_v2";

#[derive(Clone, Serialize, Deserialize)]
pub struct KillQueryRequest {
    pub plan: KillPlan,
    pub access: KillQueryAccess,
}

/// Old receivers deserialize only the flattened plan and ignore access. Old senders
/// omit access; upgraded senders include it so SUPER cannot bypass tenant checks.
#[derive(Clone, Serialize, Deserialize)]
pub struct LegacyKillQueryRequest {
    #[serde(flatten)]
    pub plan: KillPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<KillQueryAccess>,
}

pub async fn kill_query(request: LegacyKillQueryRequest) -> Result<bool> {
    // Old senders required SUPER but did not transmit a tenant. Bound this trusted
    // compatibility path to the receiver's configured tenant, never a wildcard.
    let access = request.access.unwrap_or_else(|| KillQueryAccess {
        tenant: GlobalConfig::instance()
            .query
            .tenant_id
            .tenant_name()
            .to_string(),
        user: UserIdentity::default(),
        is_super: true,
    });
    if !access.is_super {
        return Err(ErrorCode::PermissionDenied(
            "The legacy KILL action requires global SUPER privilege",
        ));
    }
    kill_query_v2(KillQueryRequest {
        plan: request.plan,
        access,
    })
    .await
}

pub async fn kill_query_v2(request: KillQueryRequest) -> Result<bool> {
    let session = create_session()?;
    let version = GlobalConfig::version();
    let query_context = session.create_query_context(version).await?;
    let interpreter = KillInterpreter::from_flight(query_context, request.plan, request.access)?;
    match interpreter.execute2().await {
        Ok(_) => Ok(true),
        Err(cause) => match cause.code() == ErrorCode::UNKNOWN_SESSION {
            true => Ok(false),
            false => Err(cause),
        },
    }
}
