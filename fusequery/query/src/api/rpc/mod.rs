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

use std::sync::Arc;

pub use actions::ExecutePlanWithShuffleAction;
use common_exception::exception::ErrorCodesBacktrace;
use common_exception::ErrorCodes;
pub use flight_client_new::FlightClient;
pub use flight_dispatcher::FlightDispatcher;
pub use flight_dispatcher::StreamInfo;
pub use flight_service_new::FlightStream;
pub use flight_service_new::FuseQueryService;
use tonic::Code;
use tonic::Status;

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedError {
    code: u16,
    message: String,
    backtrace: String,
}

pub fn to_status(error: ErrorCodes) -> Status {
    let serialized_error_json = serde_json::to_string::<SerializedError>(&SerializedError {
        code: error.code(),
        message: error.message(),
        backtrace: error.backtrace_str(),
    });

    match serialized_error_json {
        Ok(serialized_error_json) => Status::internal(serialized_error_json),
        Err(error) => Status::unknown(error.to_string()),
    }
}

pub fn from_status(status: Status) -> ErrorCodes {
    match status.code() {
        Code::Internal => match serde_json::from_str::<SerializedError>(&status.message()) {
            Err(error) => ErrorCodes::from(error),
            Ok(serialized_error) => match serialized_error.backtrace.len() {
                0 => ErrorCodes::create(serialized_error.code, serialized_error.message, None),
                _ => ErrorCodes::create(
                    serialized_error.code,
                    serialized_error.message,
                    Some(ErrorCodesBacktrace::Serialized(Arc::new(
                        serialized_error.backtrace,
                    ))),
                ),
            },
        },
        _ => ErrorCodes::UnImplement(status.to_string()),
    }
}
