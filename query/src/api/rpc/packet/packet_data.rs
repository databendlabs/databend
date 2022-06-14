use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};
use byteorder::{BigEndian, ReadBytesExt};
use futures::{Stream, StreamExt};
use common_arrow::arrow_format::flight::data::FlightData;
use common_ast::ast::DatabaseEngine::Default;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

pub enum DataPacket {
    Data(usize, FlightData),
    ErrorCode(ErrorCode),
    Progress(ProgressValues),
}

impl DataPacket {
    pub fn from_flight(data: FlightData) -> Result<DataPacket> {
        if data.app_metadata.is_empty() {
            return Err(ErrorCode::BadBytes("Flight data app metadata is empty."));
        }

        match data.app_metadata[0] {
            0x01 => DataPacket::flight_data_packet(data),
            _ => Err(ErrorCode::BadBytes("Unknown flight data packet type."))
        }
    }

    fn flight_data_packet(data: FlightData) -> Result<DataPacket> {
        if let Ok(slice) = data.app_metadata[1..].try_into() {
            return Ok(DataPacket::Data(usize::from_be_bytes(slice), FlightData {
                app_metadata: vec![],
                data_body: data.data_body,
                data_header: data.data_header,
                flight_descriptor: None,
            }));
        }

        Err(ErrorCode::BadBytes("Cannot parse inf uszie."))
    }
}

pub struct DataPacketStream {
    rx: async_channel::Receiver<DataPacket>,
}

impl DataPacketStream {
    pub fn create(rx: async_channel::Receiver<DataPacket>) -> DataPacketStream {
        DataPacketStream { rx }
    }
}

impl Stream for DataPacketStream {
    type Item = FlightData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(data_packet)) => match data_packet {
                DataPacket::Data(fragment_id, data) => Poll::Ready(Some(FlightData {
                    app_metadata: vec![0x01],
                    data_body: data.data_body,
                    data_header: data.data_header,
                    flight_descriptor: None,
                })),
                DataPacket::ErrorCode(error) => Poll::Ready(Some(FlightData {
                    app_metadata: vec![0x02],
                    data_body: error.message().into_bytes(),
                    data_header: error.code().to_be_bytes().to_vec(),
                    flight_descriptor: None,
                })),
                DataPacket::Progress(values) => {
                    // let rows = values.rows.to_be_bytes();
                    // let bytes = values.bytes.to_be_bytes();
                    Poll::Ready(Some(FlightData {
                        app_metadata: vec![0x03],
                        data_body: vec![],
                        data_header: vec![],
                        flight_descriptor: None,
                    }))
                }
            }
        }
    }
}
