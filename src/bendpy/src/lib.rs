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

#![feature(try_blocks)]

mod context;
mod datablock;
mod dataframe;
mod schema;
mod utils;

use common_config::InnerConfig;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use utils::RUNTIME;

/// A Python module implemented in Rust.
#[pymodule]
fn databend(_py: Python, m: &PyModule) -> PyResult<()> {
    RUNTIME.block_on(async {
        let mut conf: InnerConfig = InnerConfig::default();
        conf.storage.allow_insecure = true;
        GlobalServices::init(conf).await.unwrap();
    });

    m.add_class::<context::PySessionContext>()?;
    Ok(())
}
