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

pub mod client_config;
pub mod cloud_api;
pub mod notification_client;
pub mod notification_utils;
pub mod task_client;
pub mod task_utils;

#[allow(clippy::derive_partial_eq_without_eq)]
#[allow(clippy::large_enum_variant)]

/// ProtoBuf generated files.
pub mod pb {
    // taskproto is proto package name.
    tonic::include_proto!("taskproto");
    tonic::include_proto!("notificationproto");
}

pub mod utils {
    tonic::include_proto!("utils");
}

pub use prost;
