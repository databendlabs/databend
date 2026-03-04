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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;

use crate::InnerConfig;

pub struct GlobalConfig;

impl GlobalConfig {
    pub fn init(config: &InnerConfig, version: BuildInfoRef) -> Result<()> {
        GlobalInstance::set(Arc::new(config.clone()));
        GlobalInstance::set(Arc::new(version));
        Ok(())
    }

    pub fn instance() -> Arc<InnerConfig> {
        GlobalInstance::get()
    }

    pub fn version() -> BuildInfoRef {
        let version: Arc<BuildInfoRef> = GlobalInstance::get();
        version.deref()
    }

    pub fn try_get_instance() -> Option<Arc<InnerConfig>> {
        GlobalInstance::try_get()
    }

    /// Returns true if running in embedded mode (single-node, no external meta service)
    ///
    /// Embedded mode is determined by empty cluster_id and warehouse_id
    pub fn is_embedded_mode() -> bool {
        let config = Self::instance();
        config.query.common.embedded_mode
            && config.query.common.cluster_id.is_empty()
            && config.query.common.warehouse_id.is_empty()
    }
}
