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

//! Automatic table maintenance that runs after a successful write.

mod context;
mod identity;
mod resources;

pub(crate) use context::MaintenanceContext;
pub(crate) use context::run_maintenance_unit;
pub(crate) use identity::MaintenanceIdentity;
pub(crate) use resources::MaintenanceReservation;
pub(crate) use resources::MaintenanceResources;
