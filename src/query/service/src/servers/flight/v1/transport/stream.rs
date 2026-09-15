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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamSendOutcome {
    Accepted,
    ConsumerClosed,
}

#[async_trait::async_trait]
pub trait OutboundStream: Send + Sync {
    async fn send(&self, lane: usize, data: FlightData) -> Result<StreamSendOutcome>;

    /// Completes a logical stream normally. Reliable implementations wait for the peer's terminal
    /// response; legacy implementations may complete as soon as their local channel is closed.
    async fn finish(&self) -> Result<()>;

    /// Best-effort failure handshake. Cleanup must not replace the producer's original error.
    async fn fail(&self, cause: ErrorCode);

    /// Cancels without completing the logical stream.
    fn abort(&self);

    fn is_closed(&self) -> bool;
}

pub type OutboundStreamRef = Arc<dyn OutboundStream>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Accepted,
    ConsumerClosed,
}

/// Execution-side destination for one logical inbound stream.
///
/// Transport implementations own framing, sequencing, replay, and terminal handshakes. Delivery
/// adapters own payload routing, backpressure, and consumer lifecycle. Every `deliver` call
/// contains one original logical payload; transport batches are removed before crossing this seam.
#[async_trait::async_trait]
pub trait InboundDelivery: Send + Sync {
    async fn deliver(&self, lane: usize, data: FlightData) -> Result<DeliveryOutcome>;

    fn is_closed(&self) -> bool;

    /// Returns a sticky consumer-close notification when the delivery can detect idle closure.
    fn consumer_closed(&self) -> Option<BoxFuture<'static, ()>>;

    /// Releases the delivery after the logical stream reaches its first terminal state.
    fn terminate(&self, cause: Option<ErrorCode>);
}

pub(crate) fn frame_lane(lane: usize, mut data: FlightData) -> Result<FlightData> {
    let lane = u16::try_from(lane)
        .map_err(|_| ErrorCode::Internal(format!("Flight stream lane {lane} exceeds u16")))?;
    let mut metadata = lane.to_le_bytes().to_vec();
    metadata.extend_from_slice(&data.app_metadata);
    data.app_metadata = metadata.into();
    Ok(data)
}

pub(crate) fn take_lane(mut data: FlightData) -> Result<(usize, FlightData)> {
    if data.app_metadata.len() < 2 {
        return Err(ErrorCode::BadBytes(
            "Flight stream payload is missing its lane",
        ));
    }
    let lane = u16::from_le_bytes([data.app_metadata[0], data.app_metadata[1]]) as usize;
    data.app_metadata = data.app_metadata.slice(2..);
    Ok((lane, data))
}
