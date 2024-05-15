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
pub struct TIdentRaw<R, N = String> {
    /// Tenant name.
    tenant: String,

    /// Name of the resource belonging to the tenant.
    name: N,

    _p: std::marker::PhantomData<R>,
}

/// `TIdentRaw` to be Debug does not require `R` to be Debug.
impl<R, N> Debug for TIdentRaw<R, N>
where
    R: TenantResource,
    N: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        // If there is a specified type name for this alias, use it.
        // Otherwise use the default name
        let type_name = if R::TYPE.is_empty() {
            "TIdentRaw".to_string()
        } else {
            format!("{}Raw", R::TYPE)
        };

        f.debug_struct(&type_name)
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .finish()
    }
}

/// `TIdentRaw` to be Clone does not require `R` to be Clone.
impl<R, N> Clone for TIdentRaw<R, N>
where N: Clone
{
    fn clone(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            name: self.name.clone(),
            _p: Default::default(),
        }
    }
}

/// `TIdentRaw` to be PartialEq does not require `R` to be PartialEq.
impl<R, N> PartialEq for TIdentRaw<R, N>
where N: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.name == other.name
    }
}

impl<R, N> Eq for TIdentRaw<R, N> where N: PartialEq {}

impl<R, N> Hash for TIdentRaw<R, N>
where N: Hash
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.tenant, state);
        Hash::hash(&self.name, state);
        Hash::hash(&self._p, state)
    }
}

impl<R> TIdentRaw<R, String> {
    pub fn new(tenant: impl ToString, name: impl ToString) -> Self {
        Self::new_generic(tenant, name.to_string())
    }
}

impl<R> TIdentRaw<R, u64> {
    pub fn new(tenant: impl ToString, name: u64) -> Self {
        Self::new_generic(tenant, name)
    }
}

impl<R> TIdentRaw<R, ()> {
    pub fn new(tenant: impl ToString) -> Self {
        Self::new_generic(tenant, ())
    }
}

impl<R, N> TIdentRaw<R, N> {
    pub fn new_generic(tenant: impl ToString, name: N) -> Self {
        Self {
            tenant: tenant.to_string(),
            name,
            _p: Default::default(),
        }
    }

    /// Get the tenant name.
    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    /// Get the name of the resource.
    pub fn name(&self) -> &N {
        &self.name
    }

    /// Convert Self to [`TIdent`] which can be used a meta-service key.
    // TODO: load per-tenant config to determine the path in meta-service.
    pub fn to_tident(self, _config: ()) -> TIdent<R, N> {
        let tenant = Tenant::new_or_err(self.tenant, "to_tident").unwrap();
        TIdent::new_generic(tenant, self.name)
    }

    /// Create a display-able instance.
    pub fn display(&self) -> impl fmt::Display + '_
    where N: fmt::Display {
        format!("'{}'/'{}'", self.tenant, self.name)
    }
}

impl<R, N> From<TIdent<R, N>> for TIdentRaw<R, N>
where R: TenantResource
{
    fn from(ident: TIdent<R, N>) -> Self {
        let (tenant, name) = ident.unpack();
        Self {
            tenant: tenant.tenant_name().to_string(),
            name,
            _p: Default::default(),
        }
    }
}

impl<R, N> From<&TIdent<R, N>> for TIdentRaw<R, N>
where
    R: TenantResource,
    N: Clone,
{
    fn from(ident: &TIdent<R, N>) -> Self {
        Self {
            tenant: ident.tenant_name().to_string(),
            name: ident.name().clone(),
            _p: Default::default(),
        }
    }
}
