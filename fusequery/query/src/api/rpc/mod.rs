// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod flight_service_test;

#[cfg(test)]
mod flight_dispatcher_test;

#[cfg(test)]
mod flight_service_new_test;

#[macro_use]
mod macros;
mod flight_service;
mod metrics;
mod flight_service_new;
mod flight_dispatcher;
mod flight_data_stream;
mod flight_scatter;


pub use flight_service::FlightService;
pub use flight_service::FlightStream;
pub use flight_dispatcher::FlightDispatcher;
pub use flight_dispatcher::StreamInfo;


use common_exception::ErrorCodes;
use tonic::Status;

pub fn to_status(error: ErrorCodes) -> Status {
    Status::internal(error.to_string())
}
