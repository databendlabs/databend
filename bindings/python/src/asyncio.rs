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

use pyo3::prelude::*;

use pyo3_asyncio::tokio::future_into_py;

use crate::{build_connector, Connector};

/// `AsyncDatabendDriver` is the entry for all public async API
#[pyclass(module = "databend_driver")]
pub struct AsyncDatabendDriver(Connector);

#[pymethods]
impl AsyncDatabendDriver {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: &str) -> PyResult<Self> {
        Ok(AsyncDatabendDriver(build_connector(dsn)?))
    }

    /// exec
    pub fn exec<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res = this.connector.exec(&sql).await.unwrap();
            Ok(res)
        })
    }

    pub fn query_row<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let row = this.connector.query_row(&sql).await.unwrap();
            let row = row.unwrap();
            let res = row.is_empty();
            Ok(res)
        })
    }
}
