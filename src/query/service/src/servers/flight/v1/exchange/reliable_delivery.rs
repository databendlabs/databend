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

use arrow_flight::FlightData;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::future::BoxFuture;

use crate::servers::flight::v1::transport::DeliveryOutcome;
use crate::servers::flight::v1::transport::InboundDelivery;

/// Statistics destination for a reliable logical stream.
///
/// Fragment traffic routes by thread id, so it uses `NetworkInboundSender`. Statistics is a single
/// ordered stream consumed by `StatisticsReceiver`, so it only needs one queue.
pub struct StatisticsDelivery {
    sender: async_channel::Sender<std::result::Result<FlightData, ErrorCode>>,
}

impl StatisticsDelivery {
    pub fn create(
        queue_capacity: usize,
    ) -> (
        Arc<dyn InboundDelivery>,
        async_channel::Receiver<std::result::Result<FlightData, ErrorCode>>,
    ) {
        let (sender, receiver) = async_channel::bounded(queue_capacity);
        (Arc::new(Self { sender }), receiver)
    }
}

#[async_trait::async_trait]
impl InboundDelivery for StatisticsDelivery {
    async fn deliver(&self, _lane: usize, data: FlightData) -> Result<DeliveryOutcome> {
        match self.sender.send(Ok(data)).await {
            Ok(()) => Ok(DeliveryOutcome::Accepted),
            Err(_) => Ok(DeliveryOutcome::ConsumerClosed),
        }
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    fn consumer_closed(&self) -> Option<BoxFuture<'static, ()>> {
        None
    }

    fn terminate(&self, cause: Option<ErrorCode>) {
        if let Some(cause) = cause {
            let _ = self.sender.force_send(Err(cause));
        }
        self.sender.close();
    }
}
