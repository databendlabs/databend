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

use crate::tenant_key::ident::TIdent;
pub type ProcedureNameIdent = TIdent<ProcedureName, ProcedureIdentity>;

pub use kvapi_impl::ProcedureName;

use crate::principal::procedure_identity::ProcedureIdentity;
use crate::tenant::ToTenant;

impl ProcedureNameIdent {
    pub fn new(tenant: impl ToTenant, name: ProcedureIdentity) -> Self {
        Self::new_generic(tenant, name)
    }

    pub fn new_procedure(tenant: impl ToTenant, name: impl ToString, args: impl ToString) -> Self {
        Self::new(tenant, ProcedureIdentity::new(name, args))
    }

    pub fn procedure_name(&self) -> &ProcedureIdentity {
        self.name()
    }
}

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::KeyWithTenant;
    use crate::principal::ProcedureNameIdent;
    use crate::principal::procedure_id_ident::ProcedureId;
    use crate::tenant_key::resource::TenantResource;

    pub struct ProcedureName;
    impl TenantResource for ProcedureName {
        const PREFIX: &'static str = "__fd_procedure";
        const HAS_TENANT: bool = true;
        type ValueType = ProcedureId;
    }

    impl kvapi::Value for ProcedureId {
        type KeyType = ProcedureNameIdent;
        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::ProcedureNameIdent;
    use crate::principal::ProcedureIdentity;
    use crate::tenant::Tenant;

    fn test_format_parse(procedure: &str, args: &str, expect: &str) {
        let tenant = Tenant::new_literal("test_tenant");
        let procedure_ident = ProcedureIdentity::new(procedure, args);
        let tenant_procedure_ident = ProcedureNameIdent::new(tenant, procedure_ident);

        let key = tenant_procedure_ident.to_string_key();
        assert_eq!(key, expect, "'{procedure}' '{args}' '{expect}'");

        let tenant_procedure_ident_parsed = ProcedureNameIdent::from_str_key(&key).unwrap();
        assert_eq!(
            tenant_procedure_ident, tenant_procedure_ident_parsed,
            "'{procedure}' '{args}' '{expect}'"
        );
    }

    #[test]
    fn test_tenant_procedure_ident_as_kvapi_key() {
        test_format_parse("procedure", "", "__fd_procedure/test_tenant/procedure/");
        test_format_parse(
            "procedure'",
            "int,timestamp,string",
            "__fd_procedure/test_tenant/procedure%27/int%2ctimestamp%2cstring",
        );

        // With correct encoding the following two pair should not be encoded into the same string.

        test_format_parse(
            "p1'@'string",
            "string",
            "__fd_procedure/test_tenant/p1%27%40%27string/string",
        );
        test_format_parse(
            "p2",
            "int'@'string",
            "__fd_procedure/test_tenant/p2/int%27%40%27string",
        );
    }
}
