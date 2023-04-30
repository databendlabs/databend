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

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_exception::Result;

pub trait LicenseManager {
    fn init() -> Result<()>
    where Self: Sized;
    fn instance() -> Arc<Box<dyn LicenseManager>>
    where Self: Sized;
    fn is_active(&self) -> bool;
}

pub struct LicenseManagerWrapper {
    pub manager: Box<dyn LicenseManager>,
}
unsafe impl Send for LicenseManagerWrapper {}
unsafe impl Sync for LicenseManagerWrapper {}

pub fn get_license_manager() -> Arc<LicenseManagerWrapper> {
    GlobalInstance::get()
}
