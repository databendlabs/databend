// Copyright 2021 Datafuse Labs.
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

use std::convert::TryInto;

use common_arrow::arrow_format::flight::data::Ticket;
use common_base::tokio;
use common_exception::Result;
use databend_query::api::FlightTicket;
use databend_query::api::StreamTicket;

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
