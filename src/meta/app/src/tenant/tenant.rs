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

use std::fmt::Display;

use databend_common_meta_types::NonEmptyString;

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
    // #[deprecated]
    pub fn new(tenant: impl ToString) -> Self {
        Self {
            tenant: tenant.to_string(),
        }
    }

    pub fn new_or_err(tenant: impl ToString, ctx: impl Display) -> Result<Self, TenantIsEmpty> {
        let non_empty =
            NonEmptyString::new(tenant.to_string()).map_err(|_e| TenantIsEmpty::new(ctx))?;

        let t = Self {
            tenant: non_empty.as_str().to_string(),
        };

        Ok(t)
    }

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

    pub fn name(&self) -> &str {
        &self.tenant
    }

    pub fn to_nonempty(&self) -> NonEmptyString {
        NonEmptyString::new(self.tenant.clone()).unwrap()
    }

    pub fn display(&self) -> impl Display {
        format!("Tenant{}", self.tenant)
    }
}

mod kvapi_key_impl {
    use std::convert::Infallible;

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::tenant::tenant::Tenant;

    impl kvapi::Key for Tenant {
        const PREFIX: &'static str = "__fd_tenant";
        type ValueType = Infallible;

        fn parent(&self) -> Option<String> {
            None
        }

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            p.done()?;

            Ok(Self { tenant })
        }
    }
}
