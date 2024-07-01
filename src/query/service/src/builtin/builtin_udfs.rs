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

use std::collections::HashMap;

use databend_common_config::UdfConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedFunction;

pub struct BuiltinUdfs {
    udf_configs: Vec<UdfConfig>,
}

impl BuiltinUdfs {
    pub fn create(udf_configs: Vec<UdfConfig>) -> BuiltinUdfs {
        BuiltinUdfs { udf_configs }
    }

    pub fn to_meta_udfs(&self) -> Result<HashMap<String, UserDefinedFunction>> {
        for _udf_config in self.udf_configs.iter() {}
        // TODO: udf config to meta udf convert
        // parse the udf definition
        Ok(HashMap::new())
    }
}
