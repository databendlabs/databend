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

use std::sync::Arc;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

create_exception!(
    databend_client,
    Error,
    PyException,
    "databend_client related errors"
);

#[pymodule]
fn _databend_driver(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<AsyncDatabendClient>()?;
    m.add_class::<AsyncDatabendConnection>()?;
    Ok(())
}

#[pyclass(module = "databend_driver")]
pub struct AsyncDatabendClient(databend_driver::Client);

#[pymethods]
impl AsyncDatabendClient {
    #[new]
    #[pyo3(signature = (dsn))]
    pub fn new(dsn: String) -> PyResult<Self> {
        let client = databend_driver::Client::new(dsn);
        Ok(Self(client))
    }

    pub fn get_conn<'p>(&'p self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let conn = this.get_conn().await.unwrap();
            Ok(AsyncDatabendConnection(conn))
        })
    }
}

#[pyclass(module = "databend_driver")]
pub struct AsyncDatabendConnection(Box<dyn databend_driver::Connection>);

#[pymethods]
impl AsyncDatabendConnection {
    pub fn exec<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res = this.exec(&sql).await.unwrap();
            Ok(res)
        })
    }

    pub fn query_row<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let row = this.query_row(&sql).await.unwrap();
            let row = row.unwrap();
            Ok(Row(row))
        })
    }

    pub fn query_iter<'p>(&'p self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let streamer = this.query_iter(&sql).await.unwrap();
            Ok(RowIterator(Arc::new(Mutex::new(streamer))))
        })
    }
}

#[pyclass(module = "databend_driver")]
pub struct Row(databend_driver::Row);

#[pymethods]
impl Row {
    pub fn values<'p>(&'p self, py: Python<'p>) -> PyResult<PyObject> {
        let res = PyTuple::new(
            py,
            self.0
                .values()
                .into_iter()
                .map(|v| Value(v.clone()).into_py(py)), // FIXME: do not clone
        );
        Ok(res.into_py(py))
    }
}

pub struct Value(databend_driver::Value);

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            databend_driver::Value::Null => py.None(),
            databend_driver::Value::EmptyArray => {
                let list = PyList::empty(py);
                list.into_py(py)
            }
            databend_driver::Value::EmptyMap => {
                let dict = PyDict::new(py);
                dict.into_py(py)
            }
            databend_driver::Value::Boolean(b) => b.into_py(py),
            databend_driver::Value::String(s) => s.into_py(py),
            databend_driver::Value::Number(n) => {
                let v = NumberValue(n);
                v.into_py(py)
            }
            databend_driver::Value::Timestamp(_) => {
                let s = self.0.to_string();
                s.into_py(py)
            }
            databend_driver::Value::Date(_) => {
                let s = self.0.to_string();
                s.into_py(py)
            }
            databend_driver::Value::Array(inner) => {
                let list = PyList::new(py, inner.into_iter().map(|v| Value(v).into_py(py)));
                list.into_py(py)
            }
            databend_driver::Value::Map(inner) => {
                let dict = PyDict::new(py);
                for (k, v) in inner {
                    dict.set_item(Value(k).into_py(py), Value(v).into_py(py))
                        .unwrap();
                }
                dict.into_py(py)
            }
            databend_driver::Value::Tuple(inner) => {
                let tuple = PyTuple::new(py, inner.into_iter().map(|v| Value(v).into_py(py)));
                tuple.into_py(py)
            }
            databend_driver::Value::Bitmap(s) => s.into_py(py),
            databend_driver::Value::Variant(s) => s.into_py(py),
        }
    }
}

pub struct NumberValue(databend_driver::NumberValue);

impl IntoPy<PyObject> for NumberValue {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            databend_driver::NumberValue::Int8(i) => i.into_py(py),
            databend_driver::NumberValue::Int16(i) => i.into_py(py),
            databend_driver::NumberValue::Int32(i) => i.into_py(py),
            databend_driver::NumberValue::Int64(i) => i.into_py(py),
            databend_driver::NumberValue::UInt8(i) => i.into_py(py),
            databend_driver::NumberValue::UInt16(i) => i.into_py(py),
            databend_driver::NumberValue::UInt32(i) => i.into_py(py),
            databend_driver::NumberValue::UInt64(i) => i.into_py(py),
            databend_driver::NumberValue::Float32(i) => i.into_py(py),
            databend_driver::NumberValue::Float64(i) => i.into_py(py),
            databend_driver::NumberValue::Decimal128(_, _) => {
                let s = self.0.to_string();
                s.into_py(py)
            }
            databend_driver::NumberValue::Decimal256(_, _) => {
                let s = self.0.to_string();
                s.into_py(py)
            }
        }
    }
}

#[pyclass(module = "databend_driver")]
pub struct RowIterator(Arc<Mutex<databend_driver::RowIterator>>);

#[pymethods]
impl RowIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Option<PyObject>> {
        let streamer = self.0.clone();
        let future = future_into_py(py, async move {
            match streamer.lock().await.next().await {
                Some(val) => match val {
                    Err(e) => Err(PyException::new_err(format!("{}", e))),
                    Ok(ret) => Ok(Row(ret)),
                },
                None => Err(PyStopAsyncIteration::new_err("The iterator is exhausted")),
            }
        });
        Ok(Some(future?.into()))
    }
}
