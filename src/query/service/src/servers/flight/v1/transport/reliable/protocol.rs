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

// do_exchange is a logical stop-and-wait stream carried by replaceable physical Flight streams.
// Each logical stream has at most one unacknowledged request. A DATA response is sent only after
// the payload has crossed the receiver's deduplication boundary: it has been accepted by the
// logical inbound queue and the expected sequence has advanced. The sender can therefore replay
// the in-flight request without delivering its payload twice:
//
//   sender                                receiver
//      |                                     |
//      |-------- DATA(sequence=N) ---------->|  accept N and advance expected sequence
//      |<------------- ACK(N) ---------------|
//      |                                     |
//      |-------- DATA(sequence=N+1) -------->|  accept N+1
//      |          physical stream lost       |  ACK is lost
//      |                                     |
//      |       open replacement stream       |
//      |-------- DATA(sequence=N+1) -------->|  duplicate: do not deliver again
//      |<------------- ACK(N+1) -------------|
//      |                                     |
//      |------------- FINISH --------------->|  complete the logical receiver
//      |<-------- RECEIVER_CLOSED ------------|
//      |                                     |
//
// Either endpoint may also terminate the logical stream with an error. The outbound client sends
// SENDER_FAIL when its producer fails; the inbound service returns FAIL when its consumer fails.
// Physical EOF or transport failure is never logical completion. Until a response is received,
// the sender retains the encoded request so it can be replayed after reconnect.

const DATA_KIND: u8 = 1;
const ACK_KIND: u8 = 2;
const FINISH_KIND: u8 = 3;
const RECEIVER_CLOSED_KIND: u8 = 4;
const FAIL_KIND: u8 = 5;
const SENDER_FAIL_KIND: u8 = 6;
const HEADER_LEN: usize = 1 + size_of::<u64>();

pub(crate) enum DoExchangeRequest {
    Data { sequence: u64, payload: FlightData },
    Finish,
    SenderFail(ErrorCode),
}

impl DoExchangeRequest {
    pub(crate) fn data(sequence: u64, payload: FlightData) -> Self {
        Self::Data { sequence, payload }
    }

    pub(crate) fn finish() -> Self {
        Self::Finish
    }

    pub(crate) fn sender_fail(cause: ErrorCode) -> Self {
        Self::SenderFail(cause)
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
            Self::SenderFail(cause) => encode_error_packet(SENDER_FAIL_KIND, cause.clone()),
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
            SENDER_FAIL_KIND => {
                data.app_metadata = data.app_metadata.slice(HEADER_LEN..);
                Ok(Self::SenderFail(ErrorCode::try_from(data)?))
            }
            ACK_KIND | RECEIVER_CLOSED_KIND | FAIL_KIND => Err(ErrorCode::Internal(
                "received a do_exchange response packet on the request stream",
            )),
            _ => Err(unknown_packet_kind(kind)),
        }
    }
}

pub(crate) enum DoExchangeResponse {
    Ack { sequence: u64 },
    ReceiverClosed,
    Fail(ErrorCode),
}

impl DoExchangeResponse {
    pub(crate) fn ack(sequence: u64) -> Self {
        Self::Ack { sequence }
    }

    pub(crate) fn receiver_closed() -> Self {
        Self::ReceiverClosed
    }

    pub(crate) fn fail(cause: ErrorCode) -> Self {
        Self::Fail(cause)
    }

    pub(crate) fn encode(self) -> FlightData {
        match self {
            Self::Ack { sequence } => encode_control_packet(ACK_KIND, sequence),
            Self::ReceiverClosed => encode_control_packet(RECEIVER_CLOSED_KIND, 0),
            Self::Fail(cause) => encode_error_packet(FAIL_KIND, cause),
        }
    }

    pub(crate) fn decode(mut data: FlightData) -> Result<Self> {
        let (kind, sequence) = decode_header(&data)?;
        match kind {
            ACK_KIND => {
                validate_control_packet(&data)?;
                Ok(Self::Ack { sequence })
            }
            RECEIVER_CLOSED_KIND => {
                validate_control_packet(&data)?;
                Ok(Self::ReceiverClosed)
            }
            FAIL_KIND => {
                data.app_metadata = data.app_metadata.slice(HEADER_LEN..);
                Ok(Self::Fail(ErrorCode::try_from(data)?))
            }
            DATA_KIND | FINISH_KIND | SENDER_FAIL_KIND => Err(ErrorCode::Internal(
                "received a do_exchange request packet on the response stream",
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

fn encode_error_packet(kind: u8, cause: ErrorCode) -> FlightData {
    let mut data = FlightData::from(cause);
    let mut metadata = BytesMut::with_capacity(HEADER_LEN + data.app_metadata.len());
    encode_header(&mut metadata, kind, 0);
    metadata.extend_from_slice(&data.app_metadata);
    data.app_metadata = metadata.freeze();
    data
}

fn decode_header(data: &FlightData) -> Result<(u8, u64)> {
    if data.app_metadata.len() < HEADER_LEN {
        return Err(ErrorCode::Internal(
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

    Err(ErrorCode::Internal(
        "do_exchange control packet contains a data payload",
    ))
}

fn unknown_packet_kind(kind: u8) -> ErrorCode {
    ErrorCode::Internal(format!("unknown do_exchange packet kind {}", kind))
}
