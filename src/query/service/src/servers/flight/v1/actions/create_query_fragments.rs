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

use databend_common_base::match_join_handle;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_settings::Settings;
use minitrace::full_name;
use minitrace::future::FutureExt;
use minitrace::Span;

use crate::servers::flight::v1::actions::InitQueryFragmentsPlan;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub async fn create_query_fragments(fragments: InitQueryFragmentsPlan) -> Result<()> {
    let config = GlobalConfig::instance();
    let session_manager = SessionManager::instance();
    let settings = Settings::create(config.query.tenant_id.clone());
    unsafe {
        // Keep settings
        settings.unchecked_apply_changes(&fragments.executor_packet.changed_settings);
    }
    let session = session_manager.create_with_settings(SessionType::FlightRPC, settings)?;
    let session = session_manager.register_session(session)?;

    let ctx = session.create_query_context().await?;
    // Keep query id
    ctx.set_id(fragments.executor_packet.query_id.clone());
    ctx.attach_query_str(fragments.executor_packet.query_kind, "".to_string());

    let spawner = ctx.clone();
    let query_id = fragments.executor_packet.query_id.clone();
    if let Err(cause) = match_join_handle(
        spawner.spawn(
            async move {
                DataExchangeManager::instance()
                    .init_query_fragments_plan(&ctx, &fragments.executor_packet)
            }
            .in_span(Span::enter_with_local_parent(full_name!())),
        ),
    )
    .await
    {
        DataExchangeManager::instance().on_finished_query(&query_id);
        return Err(cause);
    }

    Ok(())
}
