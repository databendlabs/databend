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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::QueryFragments;

pub static INIT_QUERY_FRAGMENTS: &str = "/actions/init_query_fragments";

pub async fn init_query_fragments(fragments: QueryFragments) -> Result<()> {
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(fragments.query_id.clone());
    let _guard = ThreadTracker::tracking(tracking_payload);

    ThreadTracker::tracking_future(async move {
        // Avoid blocking runtime.
        let join_handler = databend_common_base::runtime::spawn_blocking(move || {
            let exchange_manager = DataExchangeManager::instance();
            if let Err(cause) = exchange_manager.init_query_fragments_plan(&fragments) {
                exchange_manager.on_finished_query(&fragments.query_id);
                return Err(cause);
            }

            Ok(())
        });

        match join_handler.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(join_error) => match join_error.is_panic() {
                true => std::panic::resume_unwind(join_error.into_panic()),
                false => Err(ErrorCode::TokioError("Tokio cancel error")),
            },
        }
    })
    .await
}
