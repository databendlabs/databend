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
use crate::tenant_key::raw::TIdentRaw;

pub type ProcedureIdToNameIdent = TIdent<Resource, ProcedureId>;
pub type ProcedureIdToNameIdentRaw = TIdentRaw<Resource, ProcedureId>;

pub use kvapi_impl::Resource;

use crate::principal::procedure_id_ident::ProcedureId;

impl ProcedureIdToNameIdent {
    pub fn procedure_id(&self) -> ProcedureId {
        *self.name()
    }
}

impl ProcedureIdToNameIdentRaw {
    pub fn procedure_id(&self) -> ProcedureId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::principal::ProcedureIdentity;
    use crate::principal::procedure_id_to_name::ProcedureIdToNameIdent;
    use crate::tenant_key::resource::TenantResource;

    // TODO(TIdent): parent should return Some(ProcedureIdIdent::new(self.procedure_id).to_string_key())
    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_procedure_id_to_name";
        const TYPE: &'static str = "ProcedureIdToNameIdent";
        const HAS_TENANT: bool = false;
        type ValueType = ProcedureIdentity;
    }

    impl kvapi::Value for ProcedureIdentity {
        type KeyType = ProcedureIdToNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::ProcedureIdToNameIdent;
    use crate::principal::procedure_id_ident::ProcedureId;
    use crate::tenant::Tenant;

    #[test]
    fn test_procedure_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = ProcedureIdToNameIdent::new_generic(tenant, ProcedureId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_procedure_id_to_name/3");

        assert_eq!(ident, ProcedureIdToNameIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_procedure_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = ProcedureIdToNameIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_procedure_id_to_name/3");
        //
        // assert_eq!(ident, ProcedureIdToNameIdent::from_str_key(&key).unwrap());
    }
}
