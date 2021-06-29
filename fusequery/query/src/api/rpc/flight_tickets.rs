use std::convert::TryInto;

use common_arrow::arrow_flight::Ticket;
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
