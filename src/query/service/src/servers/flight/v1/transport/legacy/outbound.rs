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

use arrow_flight::FlightData;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::warn;
use tonic::Status;

use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::transport::OutboundStream;
use crate::servers::flight::v1::transport::OutboundStreamRef;
use crate::servers::flight::v1::transport::StreamSendOutcome;

/// Existing do_get sender expressed through the logical stream interface.
pub struct LegacyOutbound {
    sender: Sender<std::result::Result<FlightData, Status>>,
}

impl LegacyOutbound {
    pub fn create(sender: Sender<std::result::Result<FlightData, Status>>) -> OutboundStreamRef {
        std::sync::Arc::new(Self { sender })
    }
}

#[async_trait::async_trait]
impl OutboundStream for LegacyOutbound {
    async fn send(&self, _lane: usize, data: FlightData) -> Result<StreamSendOutcome> {
        if self.sender.send(Ok(data)).await.is_err() {
            return Ok(StreamSendOutcome::ConsumerClosed);
        }
        Ok(StreamSendOutcome::Accepted)
    }

    async fn finish(&self) -> Result<()> {
        self.sender.close();
        Ok(())
    }

    async fn fail(&self, cause: ErrorCode) {
        match FlightData::try_from(DataPacket::ErrorCode(cause)) {
            Ok(data) => {
                let _ = self.sender.send(Ok(data)).await;
            }
            Err(error) => {
                warn!("cannot encode legacy Flight failure packet: {}", error);
            }
        }
        self.sender.close();
    }

    fn abort(&self) {
        self.sender.close();
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}
