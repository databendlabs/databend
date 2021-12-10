//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_tracing::tracing;

use crate::api::CancelAction;
use crate::api::FlightAction;
use crate::interpreters::plan_schedulers::Scheduled;
use crate::sessions::QueryContext;

pub async fn handle_error(context: &Arc<QueryContext>, scheduled: Scheduled, timeout: u64) {
    let query_id = context.get_id();
    let config = context.get_config();
    let cluster = context.get_cluster();

    for (_stream_name, scheduled_node) in scheduled {
        match cluster.create_node_conn(&scheduled_node.id, &config).await {
            Err(cause) => {
                tracing::error!(
                    "Cannot cancel action for {}, cause: {}",
                    scheduled_node.id,
                    cause
                );
            }
            Ok(mut flight_client) => {
                let cancel_action = FlightAction::CancelAction(CancelAction {
                    query_id: query_id.clone(),
                });
                let executing_action = flight_client.execute_action(cancel_action, timeout);
                if let Err(cause) = executing_action.await {
                    tracing::error!(
                        "Cannot cancel action for {}, cause:{}",
                        scheduled_node.id,
                        cause
                    );
                }
            }
        };
    }
}
