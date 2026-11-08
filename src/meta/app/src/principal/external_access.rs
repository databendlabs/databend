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

pub mod network_rule_ident {
    use databend_common_exception::ErrorCode;
    use databend_meta_client::kvapi::ValueWithName;

    use super::NetworkRule;
    use crate::tenant_key::errors::ExistError;
    use crate::tenant_key::errors::UnknownError;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_network_rule";
        const TYPE: &'static str = "NetworkRule";
        const HAS_TENANT: bool = true;
        type ValueType = NetworkRule;
    }

    impl ValueWithName for NetworkRule {
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

pub mod external_access_integration_ident {
    use databend_common_exception::ErrorCode;
    use databend_meta_client::kvapi::ValueWithName;

    use super::ExternalAccessIntegration;
    use crate::tenant_key::errors::ExistError;
    use crate::tenant_key::errors::UnknownError;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_external_access_integration";
        const TYPE: &'static str = "ExternalAccessIntegration";
        const HAS_TENANT: bool = true;
        type ValueType = ExternalAccessIntegration;
    }

    impl ValueWithName for ExternalAccessIntegration {
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
