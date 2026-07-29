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
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tonic::Status;

const MAGIC: &[u8; 4] = b"DX01";
const HEADER_LEN: usize = MAGIC.len() + 1 + std::mem::size_of::<u64>();

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum FrameKind {
    Data = 1,
    Finish = 2,
    Ack = 3,
    ReceiverFinished = 4,
    FinishAck = 5,
}

impl TryFrom<u8> for FrameKind {
    type Error = Status;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            value if value == FrameKind::Data as u8 => Ok(FrameKind::Data),
            value if value == FrameKind::Finish as u8 => Ok(FrameKind::Finish),
            value if value == FrameKind::Ack as u8 => Ok(FrameKind::Ack),
            value if value == FrameKind::ReceiverFinished as u8 => Ok(FrameKind::ReceiverFinished),
            value if value == FrameKind::FinishAck as u8 => Ok(FrameKind::FinishAck),
            value => Err(Status::invalid_argument(format!(
                "unknown do_exchange frame kind: {}",
                value
            ))),
        }
    }
}

#[derive(Debug)]
pub enum DoExchangeFrame {
    Data { sequence: u64, data: FlightData },
    Ack { sequence: u64 },
    Finish { sequence: u64 },
    FinishAck { sequence: u64 },
    ReceiverFinished,
}

impl From<DoExchangeFrame> for FlightData {
    fn from(frame: DoExchangeFrame) -> Self {
        let (kind, sequence, mut data) = match frame {
            DoExchangeFrame::Data { sequence, data } => (FrameKind::Data, sequence, data),
            DoExchangeFrame::Ack { sequence } => (FrameKind::Ack, sequence, FlightData::default()),
            DoExchangeFrame::Finish { sequence } => {
                (FrameKind::Finish, sequence, FlightData::default())
            }
            DoExchangeFrame::FinishAck { sequence } => {
                (FrameKind::FinishAck, sequence, FlightData::default())
            }
            DoExchangeFrame::ReceiverFinished => {
                (FrameKind::ReceiverFinished, 0, FlightData::default())
            }
        };

        data.app_metadata = encode_header(kind, sequence, &data.app_metadata);
        data
    }
}

impl TryFrom<FlightData> for DoExchangeFrame {
    type Error = Status;

    fn try_from(mut data: FlightData) -> Result<Self, Self::Error> {
        let (kind, sequence, payload) = decode_header(&data)?;
        match kind {
            FrameKind::Data => {
                data.app_metadata = payload;
                Ok(DoExchangeFrame::Data { sequence, data })
            }
            FrameKind::Ack => {
                ensure_empty_payload(kind, &payload)?;
                Ok(DoExchangeFrame::Ack { sequence })
            }
            FrameKind::Finish => {
                ensure_empty_payload(kind, &payload)?;
                Ok(DoExchangeFrame::Finish { sequence })
            }
            FrameKind::FinishAck => {
                ensure_empty_payload(kind, &payload)?;
                Ok(DoExchangeFrame::FinishAck { sequence })
            }
            FrameKind::ReceiverFinished => {
                ensure_empty_payload(kind, &payload)?;
                Ok(DoExchangeFrame::ReceiverFinished)
            }
        }
    }
}

fn encode_header(kind: FrameKind, sequence: u64, payload: &[u8]) -> Bytes {
    let mut metadata = BytesMut::with_capacity(HEADER_LEN + payload.len());
    metadata.put_slice(MAGIC);
    metadata.put_u8(kind as u8);
    metadata.put_u64_le(sequence);
    metadata.put_slice(payload);
    metadata.freeze()
}

fn decode_header(data: &FlightData) -> Result<(FrameKind, u64, Bytes), Status> {
    if data.app_metadata.len() < HEADER_LEN || &data.app_metadata[..MAGIC.len()] != MAGIC {
        return Err(Status::invalid_argument(
            "invalid do_exchange protocol header",
        ));
    }

    let kind = FrameKind::try_from(data.app_metadata[MAGIC.len()])?;
    let sequence_offset = MAGIC.len() + 1;
    let sequence = u64::from_le_bytes(
        data.app_metadata[sequence_offset..HEADER_LEN]
            .try_into()
            .expect("do_exchange sequence has a fixed size"),
    );

    Ok((kind, sequence, data.app_metadata.slice(HEADER_LEN..)))
}

fn ensure_empty_payload(kind: FrameKind, payload: &Bytes) -> Result<(), Status> {
    if payload.is_empty() {
        return Ok(());
    }

    Err(Status::invalid_argument(format!(
        "do_exchange {:?} frame must not contain a payload",
        kind
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_round_trip_preserves_metadata() {
        let data = FlightData {
            app_metadata: Bytes::from_static(b"payload"),
            data_body: Bytes::from_static(b"body"),
            ..Default::default()
        };

        let encoded: FlightData = DoExchangeFrame::Data { sequence: 42, data }.into();
        let DoExchangeFrame::Data { sequence, data } = DoExchangeFrame::try_from(encoded).unwrap()
        else {
            panic!("expected DATA frame");
        };

        assert_eq!(sequence, 42);
        assert_eq!(data.app_metadata, Bytes::from_static(b"payload"));
        assert_eq!(data.data_body, Bytes::from_static(b"body"));
    }

    #[test]
    fn test_control_frame_round_trip() {
        let cases = [
            DoExchangeFrame::Ack { sequence: 7 },
            DoExchangeFrame::ReceiverFinished,
            DoExchangeFrame::FinishAck { sequence: 9 },
        ];

        for expected in cases {
            let encoded: FlightData = expected.into();
            let decoded = DoExchangeFrame::try_from(encoded).unwrap();
            match decoded {
                DoExchangeFrame::Ack { sequence } => assert_eq!(sequence, 7),
                DoExchangeFrame::ReceiverFinished => {}
                DoExchangeFrame::FinishAck { sequence } => assert_eq!(sequence, 9),
                frame => panic!("unexpected control frame: {:?}", frame),
            }
        }
    }
}
