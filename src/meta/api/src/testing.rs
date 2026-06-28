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

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::types::MetaError;
use databend_meta_runtime::DatabendRuntime;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;

pub(crate) async fn new_local_meta_store() -> MetaStore {
    MetaStore::new_local_testing::<DatabendRuntime>().await
}

pub(crate) fn fixed_time(timestamp: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap()
}

pub(crate) fn tenant(name: &str) -> Tenant {
    Tenant::new_literal(name)
}

pub(crate) fn table_meta() -> TableMeta {
    TableMeta {
        schema: Arc::new(TableSchema::new(vec![TableField::new(
            "number",
            TableDataType::Number(NumberDataType::UInt64),
        )])),
        engine: "JSON".to_string(),
        created_on: fixed_time(1_000),
        updated_on: fixed_time(1_000),
        ..TableMeta::default()
    }
}

pub(crate) fn dropped_table_meta() -> TableMeta {
    TableMeta {
        drop_on: Some(fixed_time(2_000)),
        ..table_meta()
    }
}

pub(crate) trait KVPbApiTestExt: KVPbApi<Error = MetaError> {
    fn insert_pb_assert_seq<K>(
        &self,
        key: &K,
        value: K::ValueType,
        seq: u64,
    ) -> impl Future<Output = anyhow::Result<()>>
    where
        K: kvapi::Key + Clone + Send + Sync,
        K::ValueType: Clone + Debug + PartialEq + FromToProto,
    {
        async move {
            let req = UpsertPB::insert(key.clone(), value.clone());
            self.upsert_pb(&req).await?;
            self.assert_pb(key, seq, value).await
        }
    }

    fn assert_pb<K>(
        &self,
        key: &K,
        seq: u64,
        value: K::ValueType,
    ) -> impl Future<Output = anyhow::Result<()>>
    where
        K: kvapi::Key + Send + Sync,
        K::ValueType: Debug + PartialEq + FromToProto,
    {
        async move {
            let got = self
                .get_pb(key)
                .await?
                .unwrap_or_else(|| panic!("expected key: {}", key.to_string_key()));
            assert_eq!(got.seq, seq);
            assert_eq!(got.data, value);
            Ok(())
        }
    }

    fn assert_no_pb<K>(&self, key: &K) -> impl Future<Output = anyhow::Result<()>>
    where
        K: kvapi::Key + Send + Sync,
        K::ValueType: FromToProto,
    {
        async move {
            assert!(
                self.get_pb(key).await?.is_none(),
                "expected absent key: {}",
                key.to_string_key()
            );
            Ok(())
        }
    }
}

impl<T> KVPbApiTestExt for T where T: KVPbApi<Error = MetaError> + ?Sized {}
