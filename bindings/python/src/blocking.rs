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

use crate::types::{ConnectionInfo, Row, RowIterator, ServerStats};

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendClient(databend_driver::Client);

#[pymethods]
impl BlockingDatabendClient {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: String) -> PyResult<Self> {
        let client = databend_driver::Client::new(dsn);
        Ok(Self(client))
    }

    pub fn get_conn(&self) -> PyResult<BlockingDatabendConnection> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let conn = this.get_conn().await.unwrap();
            conn
        });
        Ok(BlockingDatabendConnection(ret))
    }
}

#[pyclass(module = "databend_driver")]
pub struct BlockingDatabendConnection(Box<dyn databend_driver::Connection>);

#[pymethods]
impl BlockingDatabendConnection {
    pub fn info(&self) -> PyResult<ConnectionInfo> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let info = this.info().await;
            info
        });
        Ok(ConnectionInfo::new(ret))
    }

    pub fn version(&self) -> PyResult<String> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let version = this.version().await.unwrap();
            version
        });
        Ok(ret)
    }

    pub fn exec(&self, sql: String) -> PyResult<i64> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let res = this.exec(&sql).await.unwrap();
            res
        });
        Ok(ret)
    }

    pub fn query_row(&self, sql: String) -> PyResult<Option<Row>> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let row = this.query_row(&sql).await.unwrap();
            row
        });
        Ok(ret.map(Row::new))
    }

    pub fn query_iter(&self, sql: String) -> PyResult<RowIterator> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Error creating Tokio runtime: {:?}",
                e
            ))
        })?;
        // Use the runtime to block on the synchronous operation
        let result: Result<RowIterator, PyErr> = rt.block_on(async {
            match this.query_iter(&sql).await {
                Ok(result) => Ok(RowIterator::new(result)),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Error querying: {:?}",
                    e
                ))),
            }
        });

        result.map_err(|e| e.into())
    }

    pub fn stream_load(&self, sql: String, data: Vec<Vec<String>>) -> PyResult<ServerStats> {
        let this = self.0.clone();
        let rt = tokio::runtime::Runtime::new()?;
        let ret = rt.block_on(async move {
            let data = data
                .iter()
                .map(|v| v.iter().map(|s| s.as_ref()).collect())
                .collect();
            let ss = this.stream_load(&sql, data).await.unwrap();
            ss
        });
        Ok(ServerStats::new(ret))
    }
}
