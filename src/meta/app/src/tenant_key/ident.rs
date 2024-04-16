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

use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;

use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::tenant_key::raw::TIdentRaw;
use crate::tenant_key::resource::TenantResource;
use crate::KeyWithTenant;

/// `[T]enant[Ident]` is a common meta-service key structure in form of `<PREFIX>/<TENANT>/<NAME>`.
pub struct TIdent<R> {
    tenant: Tenant,
    name: String,
    _p: std::marker::PhantomData<R>,
}

/// `TIdent` to be Debug does not require `R` to be Debug.
impl<R> Debug for TIdent<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        // If there is a specified type name for this alias, use it.
        // Otherwise use the default name
        let type_name = if R::TYPE.is_empty() {
            "TIdent"
        } else {
            R::TYPE
        };

        f.debug_struct(type_name)
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .finish()
    }
}

/// `TIdent` to be Clone does not require `R` to be Clone.
impl<R> Clone for TIdent<R> {
    fn clone(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            name: self.name.clone(),
            _p: Default::default(),
        }
    }
}

/// `TIdent` to be PartialEq does not require `R` to be PartialEq.
impl<R> PartialEq for TIdent<R> {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.name == other.name
    }
}

impl<R> Eq for TIdent<R> {}

impl<R> Hash for TIdent<R> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.tenant, state);
        Hash::hash(&self.name, state);
        Hash::hash(&self._p, state)
    }
}

impl<R> TIdent<R> {
    pub fn new(tenant: impl ToTenant, name: impl ToString) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            name: name.to_string(),
            _p: Default::default(),
        }
    }

    /// Create a new instance from TIdent of different resource definition.
    pub fn new_from<T>(other: TIdent<T>) -> Self {
        Self::new(other.tenant, other.name)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create a display-able instance.
    pub fn display(&self) -> impl fmt::Display + '_ {
        format!("'{}'/'{}'", self.tenant.tenant_name(), self.name)
    }
}

impl<R> TIdent<R>
where R: TenantResource
{
    /// Convert to the corresponding Raw key that can be stored as value,
    /// getting rid of the embedded per-tenant config.
    pub fn to_raw(&self) -> TIdentRaw<R> {
        TIdentRaw::new(self.tenant_name(), self.name())
    }
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::tenant::Tenant;
    use crate::tenant_key::ident::TIdent;
    use crate::tenant_key::resource::TenantResource;
    use crate::KeyWithTenant;

    impl<R> kvapi::Key for TIdent<R>
    where R: TenantResource
    {
        const PREFIX: &'static str = R::PREFIX;
        type ValueType = R::ValueType;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_str(self.tenant_name()).push_str(&self.name)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant = p.next_nonempty()?;
            let name = p.next_str()?;

            Ok(TIdent::new(Tenant::new_nonempty(tenant), name))
        }
    }

    impl<R> KeyWithTenant for TIdent<R>
    where R: TenantResource
    {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use databend_common_meta_kvapi::kvapi::Key;

    use crate::tenant::Tenant;
    use crate::tenant_key::ident::TIdent;
    use crate::tenant_key::resource::TenantResource;

    #[test]
    fn test_tenant_ident() {
        struct Foo;

        impl TenantResource for Foo {
            const PREFIX: &'static str = "foo";
            type ValueType = Infallible;
        }

        let tenant = Tenant::new_literal("test");
        let ident = TIdent::<Foo>::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "foo/test/test1");

        assert_eq!(ident, TIdent::<Foo>::from_str_key(&key).unwrap());
    }
}
