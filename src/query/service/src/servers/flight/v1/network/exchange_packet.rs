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
const END_OF_STREAM_KIND: u8 = 3;
const HEADER_LEN: usize = 1 + size_of::<u64>();

// FlightData has no exchange-specific type, so all interpretation stays behind this boundary
// instead of letting data and control messages share an untyped payload.
pub(crate) enum ExchangePacket {
    Data { sequence: u64, payload: FlightData },
    Ack { sequence: u64 },
    EndOfStream { sequence: u64 },
}

impl ExchangePacket {
    pub(crate) fn data(sequence: u64, payload: FlightData) -> Self {
        Self::Data { sequence, payload }
    }

    pub(crate) fn ack(sequence: u64) -> Self {
        Self::Ack { sequence }
    }

    pub(crate) fn end_of_stream(sequence: u64) -> Self {
        Self::EndOfStream { sequence }
    }

    pub(crate) fn encode(self) -> FlightData {
        match self {
            Self::Data {
                sequence,
                mut payload,
            } => {
                let mut metadata = BytesMut::with_capacity(HEADER_LEN + payload.app_metadata.len());
                encode_header(&mut metadata, DATA_KIND, sequence);
                metadata.extend_from_slice(&payload.app_metadata);
                payload.app_metadata = metadata.freeze();
                payload
            }
            Self::Ack { sequence } => encode_control_packet(ACK_KIND, sequence),
            Self::EndOfStream { sequence } => encode_control_packet(END_OF_STREAM_KIND, sequence),
        }
    }

    pub(crate) fn decode(mut data: FlightData) -> Result<Self> {
        if data.app_metadata.len() < HEADER_LEN {
            return Err(ErrorCode::Internal(
                "Logical error, do_exchange packet has an incomplete header",
            ));
        }

        let kind = data.app_metadata[0];
        let sequence_offset = 1;
        let mut sequence_bytes = [0; size_of::<u64>()];
        sequence_bytes.copy_from_slice(
            &data.app_metadata[sequence_offset..sequence_offset + size_of::<u64>()],
        );
        let sequence = u64::from_le_bytes(sequence_bytes);

        match kind {
            DATA_KIND => {
                data.app_metadata = data.app_metadata.slice(HEADER_LEN..);
                Ok(Self::Data {
                    sequence,
                    payload: data,
                })
            }
            ACK_KIND | END_OF_STREAM_KIND => {
                if data.app_metadata.len() != HEADER_LEN
                    || data.flight_descriptor.is_some()
                    || !data.data_header.is_empty()
                    || !data.data_body.is_empty()
                {
                    return Err(ErrorCode::Internal(
                        "Logical error, do_exchange control packet contains a data payload",
                    ));
                }
                if kind == ACK_KIND {
                    Ok(Self::Ack { sequence })
                } else {
                    Ok(Self::EndOfStream { sequence })
                }
            }
            _ => Err(ErrorCode::Internal(format!(
                "Logical error, unknown do_exchange packet kind {}",
                kind
            ))),
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
