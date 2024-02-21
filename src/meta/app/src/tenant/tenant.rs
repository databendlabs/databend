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

/// Tenant is not stored directly in meta-store.
///
/// It is just a type for use on the client side.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tenant {
    pub tenant: String,
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::tenant::tenant::Tenant;

    impl kvapi::Key for Tenant {
        const PREFIX: &'static str = "__fd_tenant";
        type ValueType = ();

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
