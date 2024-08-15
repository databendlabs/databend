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

use crate::types::{ConnectionInfo, DriverError, Row, RowIterator, ServerStats, VERSION};
use crate::utils::wait_for_future;

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendClient(databend_driver::Client);

#[pymethods]
impl BlockingDatabendClient {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: String) -> PyResult<Self> {
        let name = format!("databend-driver-python/{}", VERSION.as_str());
        let client = databend_driver::Client::new(dsn).with_name(name);
        Ok(Self(client))
    }

    pub fn get_conn(&self, py: Python) -> PyResult<BlockingDatabendConnection> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            this.get_conn().await.map_err(DriverError::new)
        })?;
        Ok(BlockingDatabendConnection(ret))
    }
}

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendConnection(Box<dyn databend_driver::Connection>);

#[pymethods]
impl BlockingDatabendConnection {
    pub fn info(&self, py: Python) -> PyResult<ConnectionInfo> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move { this.info().await });
        Ok(ConnectionInfo::new(ret))
    }

    pub fn version(&self, py: Python) -> PyResult<String> {
        let this = self.0.clone();
        let ret = wait_for_future(
            py,
            async move { this.version().await.map_err(DriverError::new) },
        )?;
        Ok(ret)
    }

    pub fn exec(&self, py: Python, sql: String) -> PyResult<i64> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            this.exec(&sql).await.map_err(DriverError::new)
        })?;
        Ok(ret)
    }

    pub fn query_row(&self, py: Python, sql: String) -> PyResult<Option<Row>> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            this.query_row(&sql).await.map_err(DriverError::new)
        })?;
        Ok(ret.map(Row::new))
    }

    pub fn query_all(&self, py: Python, sql: String) -> PyResult<Vec<Row>> {
        let this = self.0.clone();
        let rows = wait_for_future(py, async move {
            this.query_all(&sql).await.map_err(DriverError::new)
        })?;
        Ok(rows.into_iter().map(Row::new).collect())
    }

    pub fn query_iter(&self, py: Python, sql: String) -> PyResult<RowIterator> {
        let this = self.0.clone();
        let it = wait_for_future(py, async {
            this.query_iter(&sql).await.map_err(DriverError::new)
        })?;
        Ok(RowIterator::new(it))
    }

    pub fn stream_load(
        &self,
        py: Python,
        sql: String,
        data: Vec<Vec<String>>,
    ) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let ret = wait_for_future(py, async move {
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            this.stream_load(&sql, data).await.map_err(DriverError::new)
        })?;
        Ok(ServerStats::new(ret))
    }
}
