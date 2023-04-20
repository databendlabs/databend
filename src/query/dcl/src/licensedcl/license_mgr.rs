// Copyright 2023 Databend Cloud, Inc.
//
// Licensed as a Databend Enterprise file under the Databend Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/datafuselabs/databend/blob/main/licenses/dcl.md

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_exception::Result;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::LicenseManagerWrapper;

pub struct RealLicenseManager {}

impl LicenseManager for RealLicenseManager {
    fn init() -> Result<()> {
        let rm = RealLicenseManager {};
        let wrapper = LicenseManagerWrapper {
            manager: Box::new(rm),
        };
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn is_active(&self) -> bool {
        true
    }
}
