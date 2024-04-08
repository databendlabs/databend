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

use databend_common_meta_kvapi::kvapi;

// For doc
#[allow(unused_imports)]
use crate::tenant_key::ident::TIdent;

/// Defines the in-meta-service data for some resource that belongs to a tenant.
/// Such as `PasswordPolicy`.
///
/// It includes a prefix to store the `ValueType`.
/// This trait is used to define a concrete [`TIdent`] can be used as a `kvapi::Key`.
pub trait TenantResource {
    /// The key prefix to store in meta-service.
    const PREFIX: &'static str;

    /// The type of the value for the key [`TIdent<R: TenantResource>`](TIdent).
    type ValueType: kvapi::Value;
}
