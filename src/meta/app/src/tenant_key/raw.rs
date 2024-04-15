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

//! Defines struct to store a [`TIdent`] as value, while [`TIdent`] is the type for usage as a key.

use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;

use crate::tenant::Tenant;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;
use crate::KeyWithTenant;

/// The storage of [`TIdent`] as raw value, without per-tenant config.
///
/// Stores the tenant name and the name of the resource but can not be used a meta-service key:
/// meta-service stores per-tenant config for each tenant,
/// a [`TIdent`] must be initialized by loading the per-tenant config first to determine its path in meta-service.
///
/// And this struct does not guarantee the tenant to be valid,
/// i.e., tenant-name may be empty.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TIdentRaw<R> {
    /// Tenant name.
    tenant: String,

    /// Name of the resource belonging to the tenant.
    name: String,

    _p: std::marker::PhantomData<R>,
}

/// `TIdentRaw` to be Debug does not require `R` to be Debug.
impl<R> Debug for TIdentRaw<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        // If there is a specified type name for this alias, use it.
        // Otherwise use the default name
        let type_name = if R::TYPE.is_empty() {
            "TIdentRaw"
        } else {
            R::TYPE
        };

        f.debug_struct(type_name)
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .finish()
    }
}

/// `TIdentRaw` to be Clone does not require `R` to be Clone.
impl<R> Clone for TIdentRaw<R> {
    fn clone(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            name: self.name.clone(),
            _p: Default::default(),
        }
    }
}

/// `TIdentRaw` to be PartialEq does not require `R` to be PartialEq.
impl<R> PartialEq for TIdentRaw<R> {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.name == other.name
    }
}

impl<R> Eq for TIdentRaw<R> {}

impl<R> Hash for TIdentRaw<R> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.tenant, state);
        Hash::hash(&self.name, state);
        Hash::hash(&self._p, state)
    }
}

impl<R> TIdentRaw<R> {
    pub fn new(tenant: impl ToString, name: impl ToString) -> Self {
        Self {
            tenant: tenant.to_string(),
            name: name.to_string(),
            _p: Default::default(),
        }
    }

    /// Get the tenant name.
    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    /// Get the name of the resource.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Convert Self to [`TIdent`] which can be used a meta-service key.
    // TODO: load per-tenant config to determine the path in meta-service.
    pub fn to_tident(self, _config: ()) -> TIdent<R> {
        let tenant = Tenant::new_or_err(self.tenant, "to_tident").unwrap();
        TIdent::new(tenant, self.name)
    }

    /// Create a display-able instance.
    pub fn display(&self) -> impl fmt::Display + '_ {
        format!("'{}'/'{}'", self.tenant, self.name)
    }
}

impl<R> From<TIdent<R>> for TIdentRaw<R>
where R: TenantResource
{
    fn from(ident: TIdent<R>) -> Self {
        Self {
            tenant: ident.tenant_name().to_string(),
            name: ident.name().to_string(),
            _p: Default::default(),
        }
    }
}

impl<R> From<&TIdent<R>> for TIdentRaw<R>
where R: TenantResource
{
    fn from(ident: &TIdent<R>) -> Self {
        Self {
            tenant: ident.tenant_name().to_string(),
            name: ident.name().to_string(),
            _p: Default::default(),
        }
    }
}
