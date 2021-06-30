use std::convert::TryInto;

use common_arrow::arrow_flight::Ticket;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use tonic::Status;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct StreamTicket {
    pub query_id: String,
    pub stage_id: String,
    pub stream: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum FlightTicket {
    StreamTicket(StreamTicket),
}

impl FlightTicket {
    pub fn stream(query_id: &str, stage_id: &str, stream: &str) -> FlightTicket {
        FlightTicket::StreamTicket(StreamTicket {
            query_id: query_id.to_string(),
            stage_id: stage_id.to_string(),
            stream: stream.to_string(),
        })
    }
}

impl TryInto<FlightTicket> for Ticket {
    type Error = Status;

    fn try_into(self) -> Result<FlightTicket, Self::Error> {
        match std::str::from_utf8(&self.ticket) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(utf8_body) => match serde_json::from_str::<FlightTicket>(utf8_body) {
                Err(cause) => Err(Status::invalid_argument(cause.to_string())),
                Ok(ticket) => Ok(ticket),
            },
        }
    }
}

impl TryInto<Ticket> for FlightTicket {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Ticket, Self::Error> {
        let serialized_ticket = serde_json::to_string(&self)
            .map_err_to_code(ErrorCode::LogicalError, || {
                "Logical error: cannot serialize FlightTicket."
            })?;

        Ok(Ticket {
            ticket: serialized_ticket.as_bytes().to_vec(),
        })
    }
}
