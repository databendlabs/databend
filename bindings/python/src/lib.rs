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

mod asyncio;
mod blocking;
mod types;
mod utils;

use pyo3::prelude::*;

use crate::asyncio::{AsyncDatabendClient, AsyncDatabendConnection};
use crate::blocking::{BlockingDatabendClient, BlockingDatabendConnection};
use crate::types::{ConnectionInfo, Field, Row, RowIterator, Schema, ServerStats};

#[pymodule]
fn _databend_driver(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<AsyncDatabendClient>()?;
    m.add_class::<AsyncDatabendConnection>()?;
    m.add_class::<BlockingDatabendClient>()?;
    m.add_class::<BlockingDatabendConnection>()?;
    m.add_class::<ConnectionInfo>()?;
    m.add_class::<Schema>()?;
    m.add_class::<Row>()?;
    m.add_class::<Field>()?;
    m.add_class::<RowIterator>()?;
    m.add_class::<ServerStats>()?;
    Ok(())
}
