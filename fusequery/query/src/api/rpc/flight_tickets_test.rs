use std::convert::TryInto;

use common_arrow::arrow_flight::Ticket;
use common_exception::Result;
use common_runtime::tokio;

use crate::api::rpc::flight_tickets::StreamTicket;
use crate::api::FlightTicket;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_stream_ticket_try_into() -> Result<()> {
    let from_ticket = FlightTicket::StreamTicket(StreamTicket {
        query_id: String::from("query_id"),
        stage_id: String::from("stage_id"),
        stream: String::from("stream"),
    });

    let to_ticket: Ticket = from_ticket.try_into()?;
    let from_ticket: FlightTicket = to_ticket.try_into()?;
    match from_ticket {
        FlightTicket::StreamTicket(ticket) => {
            assert_eq!(ticket.query_id, "query_id");
            assert_eq!(ticket.stage_id, "stage_id");
            assert_eq!(ticket.stream, "stream");
        }
    };

    Ok(())
}
