// Copyright 2022 Datafuse Labs.
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

use common_base::tokio;
use common_exception::Result;
use databend_query::configs::DiskStorageConfig;
use databend_query::configs::S3StorageConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_s3() -> Result<()> {
    let mut conf = crate::tests::ConfigBuilder::create().config();

    conf.storage.storage_type = "s3".to_string();
    conf.storage.s3 = S3StorageConfig {
        region: "test".to_string(),
        endpoint_url: "http://127.0.0.1:9000".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        enable_pod_iam_policy: true,
        bucket: "bucket".to_string(),
    };

    let qctx = crate::tests::create_query_context_with_config(conf)?;

    let _ = qctx.get_storage_operator().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_fs() -> Result<()> {
    let mut conf = crate::tests::ConfigBuilder::create().config();

    conf.storage.storage_type = "fs".to_string();
    conf.storage.disk = DiskStorageConfig {
        data_path: "/tmp".to_string(),
        temp_data_path: "/tmp".to_string(),
    };

    let qctx = crate::tests::create_query_context_with_config(conf)?;

    let _ = qctx.get_storage_operator().await?;

    Ok(())
}
