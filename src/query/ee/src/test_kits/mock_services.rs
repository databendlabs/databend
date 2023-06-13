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

use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManager;

use crate::aggregating_index::RealAggregatingIndexHandler;
use crate::data_mask::RealDatamaskHandler;
use crate::storages::fuse::operations::RealVacuumHandler;
use crate::table_lock::RealTableLockHandler;
use crate::test_kits::mock_licnese_mgr::MockLicenseManager;
use crate::virtual_column::RealVirtualColumnsHandler;

pub struct MockServices;
impl MockServices {
    #[async_backtrace::framed]
    pub async fn init(_config: InnerConfig) -> Result<()> {
        MockLicenseManager::init()?;
        RealVacuumHandler::init()?;
        RealAggregatingIndexHandler::init()?;
        RealTableLockHandler::init()?;
        RealDatamaskHandler::init()?;
        RealVirtualColumnsHandler::init()?;
        Ok(())
    }
}
