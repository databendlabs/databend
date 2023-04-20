// Copyright 2023 Databend Cloud, Inc.
//
// Licensed as a Databend Enterprise file under the Databend Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/datafuselabs/databend/blob/main/licenses/dcl.md

use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManager;

use crate::licensedcl::license_mgr::RealLicenseManager;

pub struct EnterpriseServices;
impl EnterpriseServices {
    #[async_backtrace::framed]
    pub async fn init(_config: InnerConfig) -> Result<()> {
        RealLicenseManager::init()?;
        Ok(())
    }
}
