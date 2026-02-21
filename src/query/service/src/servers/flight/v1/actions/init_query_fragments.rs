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

use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::QueryFragments;

pub static INIT_QUERY_FRAGMENTS: &str = "/actions/init_query_fragments";

pub async fn init_query_fragments(fragments: QueryFragments) -> Result<()> {
    let ctx = DataExchangeManager::instance().get_query_ctx(&fragments.query_id)?;

    // Set trace flag from coordinator node
    if fragments.trace_flag {
        ctx.set_trace_flag(true);
    }

    // Set trace parent from coordinator node for distributed tracing
    if let Some(trace_parent) = &fragments.trace_parent {
        ctx.set_trace_parent(Some(trace_parent.clone()));
    }

    // Set trace filter options from coordinator node
    ctx.set_trace_filter_options(fragments.trace_filter_options.clone());

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.mem_stat = ctx.get_query_memory_tracking();
    tracking_payload.query_id = Some(fragments.query_id.clone());

    debug!("init query fragments with {:?}", fragments);

    // Avoid blocking runtime.
    let query_id = fragments.query_id.clone();
    let join_handler = ctx.try_spawn(tracking_payload.tracking(async move {
        DataExchangeManager::instance().init_query_fragments_plan(&fragments)
    }))?;

    // Flatten nested Result: both JoinError and inner error should trigger cleanup
    let result = join_handler.await.map_err(ErrorCode::from).and_then(|r| r);

    if let Err(cause) = result {
        DataExchangeManager::instance().on_finished_query(&query_id, Some(cause.clone()));
        return Err(cause);
    }

    Ok(())
}
