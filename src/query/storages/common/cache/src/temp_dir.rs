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

use std::path::PathBuf;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_config::SpillConfig;
use databend_common_exception::Result;

pub struct TempDirManager {
    disk_spill_config: Option<TempDir>,
}

impl TempDirManager {
    pub fn init(config: &SpillConfig, tenant_id: String) -> Result<()> {
        let disk_spill_config = if config.path.is_empty() {
            None
        } else {
            let path = PathBuf::from(&config.path).join(tenant_id.clone());

            let temp_dir = TempDir { path };
            temp_dir.init()?;
            Some(temp_dir)
        };

        GlobalInstance::set(Arc::new(Self { disk_spill_config }));
        Ok(())
    }

    pub fn instance() -> Arc<TempDirManager> {
        GlobalInstance::get()
    }

    pub fn get_disk_spill_config(&self) -> Option<TempDir> {
        self.disk_spill_config.clone()
    }
}

#[derive(Clone)]
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    fn init(&self) -> Result<()> {
        let _ = std::fs::remove_dir_all(&self.path);
        Ok(std::fs::create_dir_all(&self.path)?)
    }
}
