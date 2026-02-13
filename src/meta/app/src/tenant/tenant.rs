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

//! [`Tenant`] is a struct that represents a tenant with runtime config in it.
//! [`UninitTenant`] is a shadow struct without runtime config.

use std::fmt::Display;

use databend_base::non_empty::NonEmptyString;

use crate::app_error::TenantIsEmpty;

/// Tenant is not stored directly in meta-store.
///
/// It is just a type for use on the client side.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tenant {
    // TODO: consider using NonEmptyString?
    pub tenant: String,
}

impl Tenant {
    pub fn new_or_err(tenant: impl ToString, ctx: impl Display) -> Result<Self, TenantIsEmpty> {
        let non_empty =
            NonEmptyString::new(tenant.to_string()).map_err(|_e| TenantIsEmpty::new(ctx))?;

        let t = Self {
            tenant: non_empty.as_str().to_string(),
        };

        Ok(t)
    }

    /// Create a `Tenant` without checking if it is empty, if debug mode is disabled.
    ///
    /// This method should only be used for testing
    pub fn new_literal(tenant: &str) -> Self {
        debug_assert!(!tenant.is_empty());
        Self {
            tenant: tenant.to_string(),
        }
    }

    /// Create from a non-empty literal string, for testing purpose only.
    pub fn new_nonempty(tenant: NonEmptyString) -> Self {
        Self {
            tenant: tenant.as_str().to_string(),
        }
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn display(&self) -> impl Display {
        format!("Tenant{}", self.tenant)
    }
}

/// A trait convert other types to [`Tenant`].
pub trait ToTenant {
    fn to_tenant(self) -> Tenant;
}

impl ToTenant for Tenant {
    fn to_tenant(self) -> Tenant {
        self
    }
}

impl ToTenant for &Tenant {
    fn to_tenant(self) -> Tenant {
        self.clone()
    }
}

mod kvapi_key_impl {

    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::KeyBuilder;
    use databend_meta_kvapi::kvapi::KeyError;

    use crate::tenant::tenant::Tenant;

    impl kvapi::KeyCodec for Tenant {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_str(&self.tenant)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant = p.next_nonempty()?;

            Ok(Self {
                tenant: tenant.to_string(),
            })
        }
    }

    #[derive(Debug)]
    pub struct EmptyTenantValue;

    impl kvapi::Key for Tenant {
        const PREFIX: &'static str = "__fd_tenant";
        type ValueType = EmptyTenantValue;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::Value for EmptyTenantValue {
        type KeyType = Tenant;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
