// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod flight_dispatcher_test;

#[cfg(test)]
mod flight_service_test;

#[cfg(test)]
mod flight_actions_test;

#[cfg(test)]
mod flight_tickets_test;

pub use flight_actions::ShuffleAction;
pub use flight_client::FlightClient;
pub use flight_dispatcher::FuseQueryFlightDispatcher;
pub use flight_service::FuseQueryFlightService;
pub use flight_tickets::FlightTicket;

mod flight_actions;
mod flight_client;
mod flight_client_stream;
mod flight_dispatcher;
mod flight_scatter;
mod flight_service;
mod flight_service_stream;
mod flight_tickets;
