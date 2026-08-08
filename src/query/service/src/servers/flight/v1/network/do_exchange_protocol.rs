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

use std::mem::size_of;

use arrow_flight::FlightData;
use bytes::BufMut;
use bytes::BytesMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

const DATA_KIND: u8 = 1;
const ACK_KIND: u8 = 2;
const FINISH_KIND: u8 = 3;
const RECEIVER_CLOSED_KIND: u8 = 4;
const HEADER_LEN: usize = 1 + size_of::<u64>();

// Each logical stream has at most one unacknowledged request. ACK is sent only after Data has
// crossed the receiver's deduplication boundary, so reconnecting can safely replay that request:
//
//   sender                         receiver
//      |                              |
//      |------ Data(sequence=N) ----->|  accept N and advance expected sequence
//      |<------------- Ack -----------|
//      |                              |
//      |------ Data(sequence=N+1) --->|  accept N+1
//      |         connection lost      |  Ack is lost
//      |                              |
//      |       reconnect stream       |
//      |------ Data(sequence=N+1) --->|  duplicate: do not deliver again
//      |<------------- Ack -----------|
//      |                              |
//      |----------- Finish ---------->|  close the logical receiver
//      |<------ ReceiverClosed --------|
//      |                              |
//
// A physical stream ending is not logical completion. The sender completes only after receiving
// ReceiverClosed; an unacknowledged Data or Finish remains replayable across reconnects.
pub(crate) enum DoExchangeRequest {
    Data { sequence: u64, payload: FlightData },
    Finish,
}

impl DoExchangeRequest {
    pub(crate) fn data(sequence: u64, payload: FlightData) -> Self {
        Self::Data { sequence, payload }
    }

    pub(crate) fn finish() -> Self {
        Self::Finish
    }

    pub(crate) fn encode(&self) -> FlightData {
        match self {
            Self::Data { sequence, payload } => {
                let mut payload = payload.clone();
                let mut metadata = BytesMut::with_capacity(HEADER_LEN + payload.app_metadata.len());
                encode_header(&mut metadata, DATA_KIND, *sequence);
                metadata.extend_from_slice(&payload.app_metadata);
                payload.app_metadata = metadata.freeze();
                payload
            }
            Self::Finish => encode_control_packet(FINISH_KIND, 0),
        }
    }

    pub(crate) fn decode(mut data: FlightData) -> Result<Self> {
        let (kind, sequence) = decode_header(&data)?;
        match kind {
            DATA_KIND => {
                data.app_metadata = data.app_metadata.slice(HEADER_LEN..);
                Ok(Self::Data {
                    sequence,
                    payload: data,
                })
            }
            FINISH_KIND => {
                validate_control_packet(&data)?;
                Ok(Self::Finish)
            }
            ACK_KIND | RECEIVER_CLOSED_KIND => Err(ErrorCode::BadArguments(
                "Logical error, received a response packet on the do_exchange request stream",
            )),
            _ => Err(unknown_packet_kind(kind)),
        }
    }
}

pub(crate) enum DoExchangeResponse {
    Ack,
    ReceiverClosed,
}

impl DoExchangeResponse {
    pub(crate) fn ack() -> Self {
        Self::Ack
    }

    pub(crate) fn receiver_closed() -> Self {
        Self::ReceiverClosed
    }

    pub(crate) fn encode(self) -> FlightData {
        match self {
            Self::Ack => encode_control_packet(ACK_KIND, 0),
            Self::ReceiverClosed => encode_control_packet(RECEIVER_CLOSED_KIND, 0),
        }
    }

    pub(crate) fn decode(data: FlightData) -> Result<Self> {
        let (kind, _) = decode_header(&data)?;
        validate_control_packet(&data)?;
        match kind {
            ACK_KIND => Ok(Self::Ack),
            RECEIVER_CLOSED_KIND => Ok(Self::ReceiverClosed),
            DATA_KIND | FINISH_KIND => Err(ErrorCode::BadArguments(
                "Logical error, received a request packet on the do_exchange response stream",
            )),
            _ => Err(unknown_packet_kind(kind)),
        }
    }
}

fn encode_header(metadata: &mut BytesMut, kind: u8, sequence: u64) {
    metadata.put_u8(kind);
    metadata.put_u64_le(sequence);
}

fn encode_control_packet(kind: u8, sequence: u64) -> FlightData {
    let mut metadata = BytesMut::with_capacity(HEADER_LEN);
    encode_header(&mut metadata, kind, sequence);
    FlightData {
        app_metadata: metadata.freeze(),
        ..Default::default()
    }
}

fn decode_header(data: &FlightData) -> Result<(u8, u64)> {
    if data.app_metadata.len() < HEADER_LEN {
        return Err(ErrorCode::BadBytes(
            "do_exchange packet has an incomplete header",
        ));
    }

    let mut sequence_bytes = [0; size_of::<u64>()];
    sequence_bytes.copy_from_slice(&data.app_metadata[1..HEADER_LEN]);
    Ok((data.app_metadata[0], u64::from_le_bytes(sequence_bytes)))
}

fn validate_control_packet(data: &FlightData) -> Result<()> {
    if data.app_metadata.len() == HEADER_LEN
        && data.flight_descriptor.is_none()
        && data.data_header.is_empty()
        && data.data_body.is_empty()
    {
        return Ok(());
    }

    Err(ErrorCode::BadBytes(
        "do_exchange control packet contains a data payload",
    ))
}

fn unknown_packet_kind(kind: u8) -> ErrorCode {
    ErrorCode::BadBytes(format!("unknown do_exchange packet kind {}", kind))
}
