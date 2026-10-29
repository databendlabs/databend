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
use async_channel::Sender;
use async_channel::TrySendError;
use tokio::sync::Semaphore;

use super::super::wire_format::extract_tid;
use super::super::wire_format::flight_data_size;
use super::super::wire_format::is_batch;
use super::super::wire_format::split_batch_flight_data;
use super::exchange_queue::ExchangeReceiveChannels;
use super::exchange_queue::QueueItem;
use super::exchange_queue::RemoteQueueItem;

/// Routes one do_exchange connection's frames to shared per-thread receive queues.
///
/// All threads share this connection's byte quota. The router owns only senders;
/// dropping it leaves other producers usable, and the final sender closes a queue.
pub struct ExchangeFrameRouter {
    senders: Vec<Sender<QueueItem>>,
    semaphore: Arc<Semaphore>,
    max_bytes_per_connection: usize,
}

impl ExchangeFrameRouter {
    pub fn new(
        receive_channels: &ExchangeReceiveChannels,
        max_bytes_per_connection: usize,
    ) -> Self {
        Self {
            senders: receive_channels.senders.clone(),
            semaphore: Arc::new(Semaphore::new(max_bytes_per_connection)),
            max_bytes_per_connection,
        }
    }

    /// Splits a frame and routes its items by thread ID, waiting for receive quota.
    ///
    /// Returns `Err(())` when all receivers are closed so the connection can stop.
    /// Items targeting an individually closed receiver are discarded.
    #[allow(
        clippy::result_unit_err,
        reason = "Callers only need a disconnect signal"
    )]
    pub async fn route_frame(&self, frame: FlightData) -> Result<(), ()> {
        if is_batch(&frame) {
            for item in split_batch_flight_data(frame) {
                self.enqueue_item(item).await?;
            }
            Ok(())
        } else {
            self.enqueue_item(frame).await
        }
    }

    async fn enqueue_item(&self, data: FlightData) -> Result<(), ()> {
        let tid = extract_tid(&data);
        let size = flight_data_size(&data).min(self.max_bytes_per_connection);
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(size as u32)
            .await
            .expect("connection receive quota is never closed");

        let item = QueueItem::RemoteData(RemoteQueueItem::new(data, permit));
        match self.senders[tid].try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Closed(_)) if self.senders.iter().all(Sender::is_closed) => Err(()),
            Err(TrySendError::Closed(_)) => Ok(()),
            Err(TrySendError::Full(_)) => unreachable!("remote receive queues are unbounded"),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_channel::TryRecvError;
    use databend_common_expression::DataBlock;

    use super::*;
    use crate::servers::flight::v1::network::outbound::BlockSender;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;
    use crate::servers::flight::v1::network::wire_format::merge_flight_data_batch;

    fn frame(tid: u16) -> FlightData {
        FlightData {
            app_metadata: tid.to_le_bytes().to_vec().into(),
            data_body: vec![42; 8].into(),
            ..FlightData::default()
        }
    }

    #[test]
    fn test_receive_quota_is_shared_across_threads() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(2);
            let router = ExchangeFrameRouter::new(&queues, flight_data_size(&frame(0)));
            router.route_frame(frame(0)).await.unwrap();

            let mut pending = Box::pin(router.route_frame(frame(1)));
            assert!(futures::poll!(pending.as_mut()).is_pending());
            assert!(queues.receivers[1].is_empty());

            // Releasing an item from thread 0 must unblock delivery to thread 1.
            drop(queues.receivers[0].try_recv().unwrap());
            assert!(matches!(
                futures::poll!(pending.as_mut()),
                std::task::Poll::Ready(Ok(()))
            ));
            assert!(queues.receivers[1].try_recv().is_ok());
        });
    }

    #[test]
    fn test_router_drop_preserves_other_remote_and_local_producers() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(1);
            let first = ExchangeFrameRouter::new(&queues, 1024);
            let second = ExchangeFrameRouter::new(&queues, 1024);
            let local = create_local_block_senders(&queues);
            let receiver = queues.receivers[0].clone();
            drop(queues);

            drop(first);
            second.route_frame(frame(0)).await.unwrap();
            assert!(receiver.try_recv().is_ok());

            drop(second);
            local[0].send_block(DataBlock::empty()).await.unwrap();
            assert!(receiver.try_recv().is_ok());
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

            drop(local);
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Closed)));
        });
    }

    #[test]
    fn test_batch_continues_past_closed_thread() {
        futures::executor::block_on(async {
            let queues = ExchangeReceiveChannels::new(2);
            let router = ExchangeFrameRouter::new(&queues, flight_data_size(&frame(0)));
            queues.receivers[0].close();

            let batch = merge_flight_data_batch(vec![frame(0), frame(1)]);
            router.route_frame(batch).await.unwrap();
            assert!(queues.receivers[1].try_recv().is_ok());

            queues.receivers[1].close();
            assert!(router.route_frame(frame(1)).await.is_err());
        });
    }
}
