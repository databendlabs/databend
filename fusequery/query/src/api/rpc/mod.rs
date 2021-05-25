// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod flight_dispatcher_test;

#[cfg(test)]
mod flight_service_new_test;

#[cfg(test)]
mod flight_scatter_test;

#[macro_use]
mod macros;
mod metrics;
mod flight_service_new;
mod flight_dispatcher;
mod flight_data_stream;
mod flight_scatter;
mod flight_client_new;
mod actions;

pub use actions::ExecutePlanWithShuffleAction;

pub use flight_dispatcher::FlightDispatcher;
pub use flight_dispatcher::StreamInfo;
pub use flight_client_new::FlightClient;
pub use flight_service_new::FlightStream;
pub use flight_service_new::FuseQueryService;


use common_exception::ErrorCodes;
use tonic::Status;

pub fn to_status(error: ErrorCodes) -> Status {
    Status::internal(error.to_string())
}

pub fn from_status(status: Status) -> ErrorCodes {
    ErrorCodes::UnknownException(status.to_string())
}
