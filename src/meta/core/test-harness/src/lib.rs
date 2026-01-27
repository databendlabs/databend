// Copyright 2021 Datafuse Labs
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

//! Test harness and utilities for meta-service integration tests.
//!
//! This crate provides shared infrastructure for testing components that
//! interact with the meta-service, including:
//! - Test harness for async tests with logging/tracing setup
//! - MetaSrvTestContext for managing test instances
//! - Utilities for starting metasrv clusters and creating gRPC clients

mod harness;
mod service;

pub use harness::meta_service_test_harness;
pub use harness::meta_service_test_harness_sync;
pub use service::MetaSrvTestContext;
pub use service::make_grpc_client;
pub use service::start_metasrv;
pub use service::start_metasrv_cluster;
pub use service::start_metasrv_with_context;
