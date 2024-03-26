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

use databend_common_meta_kvapi::kvapi;

use crate::tenant::Tenant;

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

/// `[T]enant[Ident]` is a common meta-service key structure in form of `<PREFIX>/<TENANT>/<NAME>`.
pub struct TIdent<R> {
    tenant: Tenant,
    name: String,
    _p: std::marker::PhantomData<R>,
}

/// `TIdent` to be Debug does not require `R` to be Debug.
impl<R> Debug for TIdent<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantResourceIdent")
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
    pub fn new(tenant: Tenant, name: impl ToString) -> Self {
        Self {
            tenant,
            name: name.to_string(),
            _p: Default::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use super::TIdent;
    use crate::tenant::Tenant;
    use crate::tenant_key::TenantResource;
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
    use databend_common_meta_types::NonEmptyString;

    use crate::tenant::Tenant;
    use crate::tenant_key::TIdent;
    use crate::tenant_key::TenantResource;

    #[test]
    fn test_tenant_ident() {
        struct Foo;

        impl TenantResource for Foo {
            const PREFIX: &'static str = "foo";
            type ValueType = Infallible;
        }

        let tenant = Tenant::new_nonempty(NonEmptyString::new("test").unwrap());
        let ident = TIdent::<Foo>::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "foo/test/test1");

        assert_eq!(ident, TIdent::<Foo>::from_str_key(&key).unwrap());
    }
}
