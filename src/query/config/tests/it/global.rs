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

use common_base::base::SingletonImpl;
use common_config::Config;
use common_config::GlobalConfig;
use common_exception::Result;
use once_cell::sync::OnceCell;

struct ConfigSingleton {
    config: UnsafeCell<Option<Arc<Config>>>,
}

unsafe impl Send for ConfigSingleton {}

unsafe impl Sync for ConfigSingleton {}

static GLOBAL: OnceCell<Arc<ConfigSingleton>> = OnceCell::new();

impl SingletonImpl<Arc<Config>> for ConfigSingleton {
    fn get(&self) -> Arc<Config> {
        unsafe {
            match &*self.config.get() {
                None => panic!("GlobalConfig is not init"),
                Some(config) => config.clone(),
            }
        }
    }

    fn init(&self, value: Arc<Config>) -> Result<()> {
        unsafe {
            *(self.config.get() as *mut Option<Arc<Config>>) = Some(value);
            Ok(())
        }
    }
}

#[test]
fn test_global_config() -> Result<()> {
    let config_singleton = GLOBAL.get_or_init(|| {
        Arc::new(ConfigSingleton {
            config: UnsafeCell::new(None),
        })
    });

    let mut config = Config::default();

    GlobalConfig::init(config.clone(), config_singleton.clone())?;
    assert_eq!(GlobalConfig::instance().as_ref(), &config);

    config.cmd = "test".to_string();

    GlobalConfig::init(config.clone(), config_singleton.clone())?;
    assert_eq!(GlobalConfig::instance().as_ref(), &config);

    Ok(())
}
