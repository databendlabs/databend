// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod flight_service_test;

#[cfg(test)]
mod flight_service_new_test;

#[macro_use]
mod macros;
mod flight_service;
mod metrics;
mod flight_service_new;
mod flight_actions;
mod flight_entites;


pub use flight_service::FlightService;
pub use flight_service::FlightStream;
pub use flight_actions::FlightAction;
pub use flight_actions::ActionService;


use std::collections::HashMap;
use tokio::runtime::Runtime;
use std::sync::Arc;
use common_infallible::RwLock;
use common_arrow::arrow_flight::FlightData;
use tonic::Status;

type FlightDataReceiver = tokio::sync::mpsc::Receiver<Result<FlightData, Status>>;

pub struct StageInfo {
    pub flights: std::collections::HashMap<String, FlightDataReceiver>,
}

type StagePtr = Arc<StageInfo>;
type Stages = Arc<HashMap<String, StagePtr>>;

pub struct QueryInfo {
    pub stages: Stages,
    pub runtime: common_exception::Result<Runtime>,
}

type QueryInfoPtr = Arc<QueryInfo>;
type Queries = Arc<RwLock<HashMap<String, QueryInfoPtr>>>;
//
// pub use flight_entites::QueryInfo;
// pub use flight_entites::StageInfo;
