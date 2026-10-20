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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkRule {
    pub name: String,
    // Only HOST_PORT / EGRESS rules are supported.
    pub value_list: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalAccessIntegration {
    pub name: String,
    pub allowed_network_rules: Vec<String>,
    pub enabled: bool,
}

macro_rules! resource {
    ($module:ident, $value:ident, $prefix:literal) => {
        pub mod $module {
            use databend_common_exception::ErrorCode;
            use databend_meta_client::kvapi::ValueWithName;

            use super::$value;
            use crate::tenant_key::errors::ExistError;
            use crate::tenant_key::errors::UnknownError;
            use crate::tenant_key::resource::TenantResource;

            pub struct Resource;
            impl TenantResource for Resource {
                const PREFIX: &'static str = $prefix;
                const TYPE: &'static str = stringify!($value);
                const HAS_TENANT: bool = true;
                type ValueType = $value;
            }

            impl ValueWithName for $value {
                fn name(&self) -> &str {
                    &self.name
                }
            }

            impl From<ExistError<Resource>> for ErrorCode {
                fn from(err: ExistError<Resource>) -> Self {
                    ErrorCode::InvalidArgument(err.to_string())
                }
            }

            impl From<UnknownError<Resource>> for ErrorCode {
                fn from(err: UnknownError<Resource>) -> Self {
                    ErrorCode::InvalidArgument(err.to_string())
                }
            }
        }
    };
}

resource!(network_rule_ident, NetworkRule, "__fd_network_rule");
resource!(
    external_access_integration_ident,
    ExternalAccessIntegration,
    "__fd_external_access_integration"
);
