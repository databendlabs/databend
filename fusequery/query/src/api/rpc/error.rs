// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::FuseQueryError;

impl From<serde_json::Error> for FuseQueryError {
    fn from(err: serde_json::Error) -> Self {
        FuseQueryError::build_internal_error(err.to_string())
    }
}

impl From<prost::EncodeError> for FuseQueryError {
    fn from(err: prost::EncodeError) -> Self {
        FuseQueryError::build_internal_error(err.to_string())
    }
}

impl From<tonic::transport::Error> for FuseQueryError {
    fn from(err: tonic::transport::Error) -> Self {
        FuseQueryError::build_internal_error(err.to_string())
    }
}

impl From<tonic::Status> for FuseQueryError {
    fn from(status: tonic::Status) -> Self {
        FuseQueryError::build_flight_error(status)
    }
}

impl From<FuseQueryError> for tonic::Status {
    fn from(e: FuseQueryError) -> Self {
        tonic::Status::internal(format!("{:?}", e))
    }
}

impl From<tokio::sync::mpsc::error::SendError<Result<arrow_flight::FlightData, tonic::Status>>>
    for FuseQueryError
{
    fn from(
        err: tokio::sync::mpsc::error::SendError<Result<arrow_flight::FlightData, tonic::Status>>,
    ) -> Self {
        FuseQueryError::build_internal_error(err.to_string())
    }
}
