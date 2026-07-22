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

use chrono::DateTime;
use chrono::Utc;
use databend_meta_client::kvapi;

use crate::tenant_key::ident::TIdent;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, kvapi::KeyCodec)]
pub enum LineageDirection {
    Upstream,
    Downstream,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, kvapi::KeyCodec)]
pub enum LineageObjectType {
    /// Table-like objects that share the table namespace, including tables, views,
    /// materialized views, and name-addressed foreign tables (like iceberg table.).
    Table,
    Stage,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, kvapi::KeyCodec)]
pub enum LineageIdentity {
    Id { id: String },
    Name { name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, kvapi::KeyCodec)]
pub struct LineageObjectRef {
    pub object_type: LineageObjectType,
    pub identity: LineageIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, kvapi::KeyCodec)]
pub struct LineageKey {
    pub direction: LineageDirection,
    pub object: LineageObjectRef,
    pub related_object: LineageObjectRef,
}

pub type LineageIdent = TIdent<LineageRsc, LineageKey>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LineageKind {
    View,
    MaterializedView,
    Ctas,
    DataMovement,
    Unknown(i32),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LineageDetail {
    pub kind: LineageKind,
    pub last_query_id: Option<String>,
    pub updated_on: DateTime<Utc>,
    pub column_lineage: Vec<LineageColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LineageUpdate {
    pub tenant_name: String,
    pub upstream: LineageObjectRef,
    pub downstream: LineageObjectRef,
    pub detail: LineageDetail,
    pub mode: LineageUpdateMode,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LineageUpdateMode {
    Replace,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LineageColumn {
    pub upstream: ColumnRef,
    pub downstream: ColumnRef,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnRef {
    Id(u64),
    Name(String),
}

pub use kvapi_impl::LineageRsc;

mod kvapi_impl {
    use crate::schema::LineageDetail;
    use crate::tenant_key::resource::TenantResource;

    pub struct LineageRsc;
    impl TenantResource for LineageRsc {
        const PREFIX: &'static str = "__fd_lineage";
        const HAS_TENANT: bool = true;
        type ValueType = LineageDetail;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::LineageDirection;
    use super::LineageIdent;
    use super::LineageIdentity;
    use super::LineageKey;
    use super::LineageObjectRef;
    use super::LineageObjectType;
    use crate::tenant::Tenant;

    #[test]
    fn test_lineage_downstream_name_to_id_key() {
        let ident = LineageIdent::new_generic(Tenant::new_literal("tenant_a"), LineageKey {
            direction: LineageDirection::Downstream,
            object: LineageObjectRef {
                object_type: LineageObjectType::Stage,
                identity: LineageIdentity::Name {
                    name: "catalog/db/stage/path".to_string(),
                },
            },
            related_object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id {
                    id: "42".to_string(),
                },
            },
        });

        assert_round_trip(
            ident,
            "__fd_lineage/tenant_a/downstream/stage/name/catalog%2fdb%2fstage%2fpath/table/id/42",
        );
    }

    #[test]
    fn test_lineage_downstream_id_to_id_key() {
        let ident = LineageIdent::new_generic(Tenant::new_literal("tenant_a"), LineageKey {
            direction: LineageDirection::Downstream,
            object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id {
                    id: "11".to_string(),
                },
            },
            related_object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id {
                    id: "42".to_string(),
                },
            },
        });

        assert_round_trip(
            ident,
            "__fd_lineage/tenant_a/downstream/table/id/11/table/id/42",
        );
    }
}
