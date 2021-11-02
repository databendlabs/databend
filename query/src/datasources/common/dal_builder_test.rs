//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_dal::DataAccessorBuilder;

use crate::configs::AzureStorageBlobConfig;
use crate::configs::DiskStorageConfig;
use crate::configs::S3StorageConfig;
use crate::configs::StorageConfig;
use crate::datasources::common::ContextDalBuilder;

#[test]
fn test_dal_builder() -> common_exception::Result<()> {
    let mut storage_config = StorageConfig {
        storage_type: "disk".to_string(),
        disk: DiskStorageConfig {
            data_path: "/tmp".to_string(),
        },
        s3: S3StorageConfig {
            region: "".to_string(),
            endpoint_url: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            bucket: "".to_string(),
        },
        azure_storage_blob: AzureStorageBlobConfig {
            account: "".to_string(),
            master_key: "".to_string(),
            container: "".to_string(),
        },
    };

    let dal = ContextDalBuilder::new(storage_config.clone()).build();
    assert!(dal.is_ok());

    storage_config.storage_type = "not exists".to_string();
    let dal = ContextDalBuilder::new(storage_config).build();
    assert!(dal.is_err());

    Ok(())
}
