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

//! Node-local execution for asynchronous post-write maintenance. Writes and successful COMMITs
//! run eligible Compact inline. With `table_hook_mode=sync` (default), a single non-FINAL
//! Recluster round and hinted Analyze finish inline before notifying background index Refresh.
//! In `async` mode ordinary persistent Fuse tables queue Analyze and FINAL Recluster separately.
//! Temporary, transient and `default.system_history` tables run only the inline operations and
//! skip automatic Refresh. In particular, transient index publications must not race the next
//! transaction's uncommitted objects against zero-retention vacuum. Writes do not promise async
//! index coverage on return: Refresh checks the committed snapshot, without statement segment hints.
//!
//! The queue coalesces compatible notifications by table ID and operation. Refresh and Analyze
//! share admission with Recluster (4:1 dispatch when both classes are ready); units for one
//! table run serially on this node. A fresh reservation, not the node's mode, marks an automatic
//! execution context. The local queue is best-effort and does not replace cross-node table locks,
//! segment claims, or exact-version checks at publication.

mod context;
mod executor;
mod identity;
mod policy;
mod resources;
pub(crate) mod scheduler;
mod service;
pub(crate) mod task;

pub(crate) use context::MaintenanceContext;
pub(crate) use context::run_maintenance_unit;
pub(crate) use identity::MaintenanceIdentity;
pub(crate) use resources::MaintenanceReservation;
pub(crate) use resources::MaintenanceResources;
pub(crate) use service::MaintenanceShutdown;
pub(crate) use service::TableMaintenanceService;
