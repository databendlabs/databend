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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use tokio::sync::Semaphore;

use super::NetworkOutbound;
use super::PendingNetworkOutbound;
use super::SendOutcome;

#[derive(Clone)]
pub struct BlockOutboundConfig {
    pub queue_capacity_factor: usize,
    pub max_batch_bytes: usize,
}

impl Default for BlockOutboundConfig {
    fn default() -> Self {
        Self {
            queue_capacity_factor: 64,
            max_batch_bytes: 256 * 1024,
        }
    }
}

pub struct BlockOutboundSet {
    outbounds: Vec<Arc<NetworkOutbound>>,
    remaining_producers: AtomicUsize,
}

impl BlockOutboundSet {
    pub(crate) fn create_with_producers(
        outbounds: Vec<PendingNetworkOutbound>,
        num_producers: usize,
        config: BlockOutboundConfig,
        runtime: &Runtime,
    ) -> Self {
        let slots = Arc::new(Semaphore::new(
            config.queue_capacity_factor * outbounds.len().max(1),
        ));
        let outbounds = outbounds
            .into_iter()
            .map(|outbound| {
                Arc::new(outbound.start(slots.clone(), Some(config.max_batch_bytes), runtime))
            })
            .collect();

        Self {
            outbounds,
            remaining_producers: AtomicUsize::new(num_producers),
        }
    }

    pub async fn finish_producer(&self) -> Result<()> {
        if self.remaining_producers.fetch_sub(1, Ordering::AcqRel) != 1 {
            return Ok(());
        }

        for result in
            futures::future::join_all(self.outbounds.iter().map(|outbound| outbound.finish())).await
        {
            result?;
        }
        Ok(())
    }

    pub async fn fail(&self, cause: ErrorCode) {
        futures::future::join_all(
            self.outbounds
                .iter()
                .map(|outbound| outbound.fail(cause.clone())),
        )
        .await;
    }

    pub async fn send(&self, tid: usize, dest_idx: usize, data: FlightData) -> Result<SendOutcome> {
        self.outbounds[dest_idx].send(tid, data).await
    }

    pub fn is_closed(&self, dest_idx: usize) -> bool {
        self.outbounds[dest_idx].is_closed()
    }
}
