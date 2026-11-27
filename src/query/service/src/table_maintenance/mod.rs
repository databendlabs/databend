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
//! run eligible Compact and Analyze inline, with Analyze after the original write lock is released.
//! Ordinary persistent Fuse tables queue index Refresh and independently request FINAL Recluster
//! after committed logical writes. After the accumulation window, a pending Recluster batch needs
//! enough measured writes or an idle interval confirmed by all warehouse members to start. Batches
//! on this node share fresh probe results for the same warehouse. A batch expires if neither
//! condition is met within ten minutes after the window. Once started, FINAL needs only resource
//! admission for subsequent rounds and does not accumulate a follow-up for the same key.
//! Temporary, transient and `default.system_history` tables run only the inline operations.
//! In particular, transient index publications must not race the next transaction's uncommitted
//! objects against zero-retention vacuum. Writes do not promise async index coverage on return:
//! Refresh checks current segment paths added since the parent of the first queued write.
//! It does not backfill unchanged segments or recover lost notifications.
//!
//! The queue coalesces compatible notifications by table ID and operation. Refresh shares
//! admission with Recluster (4:1 dispatch when both classes are ready); units for one
//! table run serially on this node. A fresh reservation marks an automatic execution context.
//! The local queue is best-effort and does not replace exact-version checks at publication or
//! Recluster's existing cross-node coordination.

mod context;
mod identity;
mod resources;
pub(crate) mod scheduler;
mod service;
pub(crate) mod task;

pub(crate) use context::MaintenanceContext;
pub(crate) use identity::MaintenanceIdentity;
pub(crate) use resources::MaintenanceReservation;
pub(crate) use resources::MaintenanceResources;
pub(crate) use service::ForegroundRequest;
pub(crate) use service::MaintenanceShutdown;
pub(crate) use service::TableMaintenanceService;
