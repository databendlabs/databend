// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license_manager::LicenseManager;

use crate::aggregating_index::RealAggregatingIndexHandler;
use crate::attach_table::RealAttachTableHandler;
use crate::data_mask::RealDatamaskHandler;
use crate::fail_safe::RealFailSafeHandler;
use crate::hilbert_clustering::RealHilbertClusteringHandler;
use crate::license::license_mgr::RealLicenseManager;
use crate::resource_management::init_resources_management;
use crate::storage_encryption::RealStorageEncryptionHandler;
use crate::storage_quota::RealStorageQuotaHandler;
use crate::storages::fuse::operations::RealVacuumHandler;
use crate::stream::RealStreamHandler;
use crate::table_index::RealTableIndexHandler;
use crate::virtual_column::RealVirtualColumnHandler;

pub struct EnterpriseServices;
impl EnterpriseServices {
    #[async_backtrace::framed]
    pub async fn init(cfg: InnerConfig) -> Result<()> {
        RealLicenseManager::init(cfg.query.tenant_id.tenant_name().to_string())?;
        RealStorageEncryptionHandler::init(&cfg)?;
        RealVacuumHandler::init()?;
        RealAggregatingIndexHandler::init()?;
        RealDatamaskHandler::init()?;
        RealVirtualColumnHandler::init()?;
        RealStreamHandler::init()?;
        RealAttachTableHandler::init()?;
        RealTableIndexHandler::init()?;
        RealStorageQuotaHandler::init(&cfg)?;
        RealFailSafeHandler::init()?;
        init_resources_management(&cfg).await?;
        RealHilbertClusteringHandler::init()?;
        Ok(())
    }
}
