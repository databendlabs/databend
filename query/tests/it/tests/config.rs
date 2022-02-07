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

use databend_query::configs::Config;

pub struct ConfigBuilder {
    conf: Config,
}

impl ConfigBuilder {
    pub fn create() -> ConfigBuilder {
        let mut conf = Config::default();
        conf.query.tenant_id = "test".to_string();

        ConfigBuilder { conf }
    }

    pub fn with_management_mode(&self) -> ConfigBuilder {
        let mut conf = self.conf.clone();
        conf.query.management_mode = true;
        ConfigBuilder { conf }
    }

    pub fn config(&self) -> Config {
        self.conf.clone()
    }
}
