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

use std::cell::UnsafeCell;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_exception::Result;

use crate::accessor::SharingAccessor;
use crate::configs::Config;

// hold singleton services.
pub struct SharingServices {
    // by default, operator init needs the global_runtime singleton instance
    // thus we keep it in the sharing services, alternatively, we may consider expose no layer operator
    global_runtime: UnsafeCell<Option<Arc<Runtime>>>,
    // storage_accessor is the accessor for the shared tenant data.
    storage_accessor: UnsafeCell<Option<Arc<SharingAccessor>>>,
}

unsafe impl Send for SharingServices {}

unsafe impl Sync for SharingServices {}

impl SharingServices {
    pub async fn init(config: Config) -> Result<()> {
        let sharing_services = Arc::new(SharingServices {
            global_runtime: UnsafeCell::new(None),
            storage_accessor: UnsafeCell::new(None),
        });
        GlobalIORuntime::init(config.storage.num_cpus as usize, sharing_services.clone())?;
        SharingAccessor::init(&config, sharing_services.clone()).await
    }
}

impl SingletonImpl<Arc<SharingAccessor>> for SharingServices {
    fn get(&self) -> Arc<SharingAccessor> {
        unsafe {
            match &*self.storage_accessor.get() {
                None => panic!("Sharing Accessor is not init"),
                Some(storage_accessor) => storage_accessor.clone(),
            }
        }
    }

    fn init(&self, value: Arc<SharingAccessor>) -> Result<()> {
        unsafe {
            *(self.storage_accessor.get() as *mut Option<Arc<SharingAccessor>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<Runtime>> for SharingServices {
    fn get(&self) -> Arc<Runtime> {
        unsafe {
            match &*self.global_runtime.get() {
                None => panic!("Global Runtime is not init"),
                Some(runtime) => runtime.clone(),
            }
        }
    }

    fn init(&self, value: Arc<Runtime>) -> Result<()> {
        unsafe {
            *(self.global_runtime.get() as *mut Option<Arc<Runtime>>) = Some(value);
            Ok(())
        }
    }
}
