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

use databend_meta_kvapi::kvapi;

// For doc
#[allow(unused_imports)]
use crate::tenant_key::ident::TIdent;

/// Defines the in-meta-service data for some resource that belongs to a tenant.
/// Such as `PasswordPolicy`.
///
/// It includes a prefix to store the `ValueType`.
/// This trait is used to define a concrete [`TIdent`] can be used as a `kvapi::Key`.
pub trait TenantResource: 'static {
    /// The key prefix to store in meta-service.
    const PREFIX: &'static str;

    /// The type name of the alias struct.
    ///
    /// For example, for the follow type alias, `Resource::TYPE` is `Foo`:
    /// ```rust,ignore
    /// type Foo = TIdent<Resource>;
    /// ```
    const TYPE: &'static str = "";

    /// Whether to encode tenant into the key, when encoding a key without key-space.
    ///
    /// There are two kinds of key before introducing key space:
    /// 1. The key with tenant, such as `CatalogNameIdent(tenant, name)`.
    /// 2. The key without tenant, such as `TableId(table_id:u64)`.
    ///
    /// Meta-service keys are implemented with [`TIdent`].
    /// [`TenantResource`] should be able to distinguish the two kinds of keys.
    const HAS_TENANT: bool;

    /// The type of the value for the key [`TIdent<R: TenantResource>`](TIdent).
    type ValueType: kvapi::Value;
}
