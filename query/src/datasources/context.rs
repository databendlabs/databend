// Copyright 2021 Datafuse Labs.
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
use std::str::FromStr;
use std::sync::Arc;

use common_dal::AzureBlobAccessor;
use common_dal::DalWithMetric;
use common_dal::DataAccessor;
use common_dal::InMemoryData;
use common_dal::Local;
use common_dal::StorageScheme;
use common_dal::S3;
use common_exception::Result;
use common_infallible::RwLock;
use common_metrics::TenantLabel;

use crate::configs::AzureStorageBlobConfig;
use crate::configs::StorageConfig;

/// Datasource Table Context.
#[derive(Clone, Debug, Default)]
pub struct TableContext {
    pub in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
}

impl TableContext {
    pub fn get_in_memory_data(&self) -> Result<Arc<RwLock<InMemoryData<u64>>>> {
        Ok(self.in_memory_data.clone())
    }
}

/// Datasource Context.
#[derive(Clone, Debug)]
pub struct DataSourceContext {
    tenant_label: TenantLabel,
    storage_conf: StorageConfig,
}

impl DataSourceContext {
    pub fn create(
        tenant_id: impl Into<String>,
        cluster_id: impl Into<String>,
        storage_conf: StorageConfig,
    ) -> Self {
        DataSourceContext {
            tenant_label: TenantLabel::new(tenant_id, cluster_id),
            storage_conf,
        }
    }

    /// Get the DataAccessor by the Storage schema.
    pub fn get_data_accessor(&self) -> Result<Arc<dyn DataAccessor>> {
        let conf = &self.storage_conf;
        let scheme_name = &conf.storage_type;
        let scheme = StorageScheme::from_str(scheme_name)?;
        let da: Arc<dyn DataAccessor> = match scheme {
            StorageScheme::S3 => {
                let conf = &conf.s3;
                Arc::new(S3::try_create(
                    &conf.region,
                    &conf.endpoint_url,
                    &conf.bucket,
                    &conf.access_key_id,
                    &conf.secret_access_key,
                )?)
            }
            StorageScheme::AzureStorageBlob => {
                let conf: &AzureStorageBlobConfig = &conf.azure_storage_blob;
                Arc::new(AzureBlobAccessor::with_credentials(
                    &conf.account,
                    &conf.container,
                    &conf.master_key,
                ))
            }
            StorageScheme::LocalFs => Arc::new(Local::new(conf.disk.data_path.as_str())),
        };

        Ok(Arc::new(DalWithMetric::new(self.tenant_label.clone(), da)))
    }
}
