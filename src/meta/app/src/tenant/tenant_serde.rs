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

use crate::tenant::Tenant;

/// A duplicate of [`Tenant`] struct for transport with serde support.
///
/// This struct is meant not to provide any functionality [`Tenant`] struct provides
/// and is only used for transport.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, derive_more::Display, serde::Serialize, serde::Deserialize,
)]
#[display(fmt = "TenantSerde{{{tenant}}}")]
pub struct TenantSerde {
    tenant: String,
}

impl From<Tenant> for TenantSerde {
    fn from(value: Tenant) -> Self {
        Self {
            tenant: value.tenant,
        }
    }
}

impl From<TenantSerde> for Tenant {
    fn from(value: TenantSerde) -> Self {
        Tenant {
            tenant: value.tenant,
        }
    }
}
