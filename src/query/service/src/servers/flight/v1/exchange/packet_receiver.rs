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
use async_channel::Receiver;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::super::packets::DataPacket;
use super::super::transport::legacy::LegacyInbound;
use super::exchange_packet_receiver::ExchangePacketReceiver;
use super::inbound_quota::QueueItem;

enum PacketInput {
    Legacy(LegacyInbound),
    ResultQueue(Receiver<std::result::Result<FlightData, ErrorCode>>),
    InboundQueue(Arc<ExchangePacketReceiver>),
}

pub(super) struct PacketReceiver {
    input: PacketInput,
}

impl PacketReceiver {
    pub(super) fn from_legacy(input: LegacyInbound) -> Self {
        Self {
            input: PacketInput::Legacy(input),
        }
    }

    pub(super) fn from_result_queue(
        receiver: Receiver<std::result::Result<FlightData, ErrorCode>>,
    ) -> Self {
        Self {
            input: PacketInput::ResultQueue(receiver),
        }
    }

    pub(super) fn from_inbound_queue(input: Arc<ExchangePacketReceiver>) -> Self {
        Self {
            input: PacketInput::InboundQueue(input),
        }
    }

    pub(super) async fn recv(&self) -> Result<Option<DataPacket>> {
        let data = match &self.input {
            PacketInput::Legacy(input) => input.recv().await?,
            PacketInput::ResultQueue(receiver) => match receiver.recv().await {
                Err(_) => None,
                Ok(result) => Some(result?),
            },
            PacketInput::InboundQueue(receiver) => match receiver.recv_raw().await? {
                None => None,
                Some(QueueItem::RemoteData(item)) => Some(item.into_data()),
                Some(QueueItem::LocalData(_)) => {
                    return Err(ErrorCode::Internal(
                        "PacketReceiver received a local block on a network receiver",
                    ));
                }
            },
        };
        data.map(DataPacket::try_from).transpose()
    }

    pub(super) fn close(&self) {
        match &self.input {
            PacketInput::Legacy(input) => input.close(),
            PacketInput::ResultQueue(receiver) => {
                receiver.close();
            }
            PacketInput::InboundQueue(receiver) => receiver.close(),
        }
    }
}
