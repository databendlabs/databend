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

use crate::principal::OwnershipObject;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// The meta-service key of object whose ownership to grant.
///
/// It could be a tenant's database, a tenant's table etc.
/// It is in form of `__fd_object_owners/<tenant>/<object>`.
/// where `<object>` could be:
/// - `database-by-id/<db_id>`
/// - `database-by-catalog-id/<catalog>/<db_id>`
/// - `table-by-id/<table_id>`
/// - `table-by-catalog-id/<catalog>/<table_id>`
/// - `stage-by-name/<stage_name>`
/// - `udf-by-name/<udf_name>`
pub type TenantOwnershipObjectIdent = TIdent<Resource, OwnershipObject>;
pub type TenantOwnershipObjectIdentRaw = TIdentRaw<Resource, OwnershipObject>;

pub use kvapi_impl::Resource;

use crate::tenant::ToTenant;

impl TenantOwnershipObjectIdent {
    pub fn new(tenant: impl ToTenant, subject: OwnershipObject) -> Self {
        // Legacy issue: Assert compatibility: No other catalog should be used.
        // Assertion is disabled.
        // Instead, accessing field `object` is disallowed.
        // match &subject {
        //     OwnershipObject::Database { catalog_name, .. } => {
        //         assert_eq!(catalog_name, OwnershipObject::DEFAULT_CATALOG);
        //     }
        //     OwnershipObject::Table { catalog_name, .. } => {
        //         assert_eq!(catalog_name, OwnershipObject::DEFAULT_CATALOG);
        //     }
        //     OwnershipObject::Stage { .. } => {}
        //     OwnershipObject::UDF { .. } => {}
        // }

        Self::new_unchecked(tenant, subject)
    }

    pub(crate) fn new_unchecked(tenant: impl ToTenant, object: OwnershipObject) -> Self {
        Self::new_generic(tenant, object)
    }

    // Because the encoded string key does not contain all of the field:
    // Catalog and db_id is missing, so we can not rebuild the complete key from string key.
    // Thus it is not allow to access this field.
    // pub fn subject(&self) -> &OwnershipObject {
    //     &self.object
    // }

    pub fn ownership_object(&self) -> &OwnershipObject {
        self.name()
    }
}

impl TenantOwnershipObjectIdentRaw {
    pub fn ownership_object(&self) -> &OwnershipObject {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::principal::OwnershipInfo;
    use crate::principal::TenantOwnershipObjectIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_object_owners";
        const TYPE: &'static str = "TenantOwnershipObjectIdent";
        const HAS_TENANT: bool = true;
        type ValueType = OwnershipInfo;
    }

    impl kvapi::Value for OwnershipInfo {
        type KeyType = TenantOwnershipObjectIdent;
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
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::KeyWithTenant;
    use crate::principal::OwnershipObject;
    use crate::principal::TenantOwnershipObjectIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_tenant_ownership_object_tenant_prefix() {
        let r = TenantOwnershipObjectIdent::new(
            Tenant::new_literal("tenant"),
            OwnershipObject::Database {
                catalog_name: "cat".to_string(),
                db_id: 1,
            },
        );

        assert_eq!("__fd_object_owners/tenant/", r.tenant_prefix());
    }

    #[test]
    fn test_role_grantee_as_kvapi_key() {
        // db with default catalog
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Database {
                    catalog_name: "default".to_string(),
                    db_id: 1,
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/database-by-id/1", key);

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // db with catalog
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Database {
                    catalog_name: "cata/foo".to_string(),
                    db_id: 1,
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!(
                "__fd_object_owners/test/database-by-catalog-id/cata%2ffoo/1",
                key
            );

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // table with default catalog
        {
            let obj = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Table {
                    catalog_name: "default".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
            );

            let key = obj.to_string_key();
            assert_eq!("__fd_object_owners/test/table-by-id/2", key);

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(
                TenantOwnershipObjectIdent::new_unchecked(
                    Tenant::new_literal("test"),
                    OwnershipObject::Table {
                        catalog_name: "default".to_string(),
                        db_id: 0, // db_id is not encoded into key
                        table_id: 2,
                    }
                ),
                parsed
            );
        }

        // table with catalog
        {
            let obj = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Table {
                    catalog_name: "cata/foo".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
            );

            let key = obj.to_string_key();
            assert_eq!(
                "__fd_object_owners/test/table-by-catalog-id/cata%2ffoo/2",
                key
            );

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(
                TenantOwnershipObjectIdent::new_unchecked(
                    Tenant::new_literal("test"),
                    OwnershipObject::Table {
                        catalog_name: "cata/foo".to_string(),
                        db_id: 0, // db_id is not encoded into key
                        table_id: 2,
                    }
                ),
                parsed
            );
        }

        // stage
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Stage {
                    name: "foo".to_string(),
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/stage-by-name/foo", key);

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // udf
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::UDF {
                    name: "foo".to_string(),
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/udf-by-name/foo", key);

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // warehouse
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Warehouse {
                    id: "n87s".to_string(),
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/warehouse-by-id/n87s", key);

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // masking policy
        {
            let role_grantee = TenantOwnershipObjectIdent::new_unchecked(
                Tenant::new_literal("tenant_mask"),
                OwnershipObject::MaskingPolicy { policy_id: 99 },
            );

            let key = role_grantee.to_string_key();
            assert_eq!(
                "__fd_object_owners/tenant_mask/masking-policy-by-id/99",
                key
            );

            let parsed = TenantOwnershipObjectIdent::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }
    }

    #[test]
    fn test_ownership_seq_list_key() {
        use databend_meta_kvapi::kvapi::Key;
        let obj = OwnershipObject::Sequence {
            name: "seq1".to_string(),
        };

        let ident = TenantOwnershipObjectIdent::new(Tenant::new_literal("tenant1"), obj);
        let dir_name = kvapi::DirName::new(ident);
        assert_eq!(
            dir_name.to_string_key(),
            "__fd_object_owners/tenant1/sequence-by-name"
        );
    }

    #[test]
    fn test_ownership_procedure_list_key() {
        use databend_meta_kvapi::kvapi::Key;
        let obj = OwnershipObject::Procedure { procedure_id: 1 };

        let ident = TenantOwnershipObjectIdent::new(Tenant::new_literal("tenant1"), obj);
        let dir_name = kvapi::DirName::new(ident);
        assert_eq!(
            dir_name.to_string_key(),
            "__fd_object_owners/tenant1/procedure-by-id"
        );
    }

    #[test]
    fn test_tenant_ownership_object_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = TenantOwnershipObjectIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_background_job_by_id/3");
        //
        // assert_eq!(ident, TenantOwnershipObjectIdent::from_str_key(&key).unwrap());
    }
}
