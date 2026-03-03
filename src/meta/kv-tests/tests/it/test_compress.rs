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

use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_api::kv_pb_api::compress;
use databend_common_meta_api::kv_pb_api::compress::COMPRESS_THRESHOLD;
use databend_common_meta_app::schema::CatalogIdIdent;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::HiveCatalogOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime_api::TokioRuntime;

fn large_catalog_meta() -> CatalogMeta {
    CatalogMeta {
        catalog_option: CatalogOption::Hive(HiveCatalogOption {
            address: "x".repeat(COMPRESS_THRESHOLD + 1024),
            storage_params: None,
        }),
        created_on: Default::default(),
    }
}

fn small_catalog_meta() -> CatalogMeta {
    CatalogMeta {
        catalog_option: CatalogOption::Hive(HiveCatalogOption {
            address: "127.0.0.1:10000".to_string(),
            storage_params: None,
        }),
        created_on: Default::default(),
    }
}

/// Integration test for the full upsert_pb → raw KV → get_pb path with compression.
#[tokio::test]
async fn test_compress() -> anyhow::Result<()> {
    compress::GLOBAL_ENCODER.set_compress(true);

    let store = MetaStore::new_local_testing::<TokioRuntime>().await;
    let tenant = Tenant::new_literal("dummy");

    // Large values must be stored with the zstd compression header.
    {
        let key = CatalogIdIdent::new(&tenant, 1);

        store
            .upsert_pb(&UpsertPB::update(key.clone(), large_catalog_meta()))
            .await?;

        let raw_bytes: Vec<u8> = store.get_kv(&key.to_string_key()).await?.unwrap().data;

        assert_eq!(
            &raw_bytes[..4],
            &[0x0F, 0x01, 0x00, 0x00],
            "large values must start with the zstd compression header [0x0F, FLAG_ZSTD, 0, 0]"
        );
    }

    // Small values must be stored as raw protobuf (no compression header).
    {
        let key = CatalogIdIdent::new(&tenant, 2);

        store
            .upsert_pb(&UpsertPB::update(key.clone(), small_catalog_meta()))
            .await?;

        let raw_bytes: Vec<u8> = store.get_kv(&key.to_string_key()).await?.unwrap().data;

        assert_ne!(
            raw_bytes.first().copied(),
            Some(0x0F),
            "small values must not have the compression header"
        );
    }

    // Large values must round-trip correctly through get_pb.
    {
        let key = CatalogIdIdent::new(&tenant, 3);
        let meta = large_catalog_meta();

        store
            .upsert_pb(&UpsertPB::update(key.clone(), meta.clone()))
            .await?;

        let got = store.get_pb(&key).await?.unwrap();

        assert_eq!(got.data, meta, "round-trip must recover the original value");
    }

    Ok(())
}
