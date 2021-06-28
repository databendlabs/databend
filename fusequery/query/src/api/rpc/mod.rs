// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod flight_dispatcher_test;

#[cfg(test)]
mod flight_service_new_test;

mod actions;
mod flight_client_new;
mod flight_data_stream;
mod flight_dispatcher;
mod flight_scatter;
mod flight_service_new;

pub use actions::ExecutePlanWithShuffleAction;
use common_exception::ErrorCode;
pub use flight_client_new::FlightClient;
pub use flight_dispatcher::FlightDispatcher;
pub use flight_dispatcher::StreamInfo;
pub use flight_service_new::FlightStream;
pub use flight_service_new::FuseQueryService;
use tonic::Status;

pub fn to_status(error: ErrorCode) -> Status {
    error.into()
}

pub fn from_status(status: Status) -> ErrorCode {
    status.into()
}
