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

pub mod tenant_quota_ident;

mod quota;
#[allow(clippy::module_inception)]
mod tenant;
mod uninit_tenant;

pub use quota::TenantQuota;
pub use tenant::Tenant;
pub use tenant::ToTenant;
pub use tenant_quota_ident::TenantQuotaIdent;
pub use uninit_tenant::UninitTenant;
