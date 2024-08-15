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

use chrono::{NaiveDate, NaiveDateTime};
use once_cell::sync::Lazy;
use pyo3::exceptions::{PyException, PyStopAsyncIteration, PyStopIteration};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple, PyType};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

use crate::utils::wait_for_future;

pub static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

pub static DECIMAL_CLS: GILOnceCell<Py<PyType>> = GILOnceCell::new();

fn get_decimal_cls(py: Python<'_>) -> PyResult<&Bound<PyType>> {
    DECIMAL_CLS
        .get_or_try_init(py, || {
            py.import_bound(intern!(py, "decimal"))?
                .getattr(intern!(py, "Decimal"))?
                .extract()
        })
        .map(|ty| ty.bind(py))
}

pub struct Value(databend_driver::Value);

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            databend_driver::Value::Null => py.None(),
            databend_driver::Value::EmptyArray => {
                let list = PyList::empty_bound(py);
                list.into_py(py)
            }
            databend_driver::Value::EmptyMap => {
                let dict = PyDict::new_bound(py);
                dict.into_py(py)
            }
            databend_driver::Value::Boolean(b) => b.into_py(py),
            databend_driver::Value::Binary(b) => {
                let buf = PyBytes::new_bound(py, &b);
                buf.into_py(py)
            }
            databend_driver::Value::String(s) => s.into_py(py),
            databend_driver::Value::Number(n) => {
                let v = NumberValue(n);
                v.into_py(py)
            }
            databend_driver::Value::Timestamp(_) => {
                let t = NaiveDateTime::try_from(self.0).unwrap();
                t.into_py(py)
            }
            databend_driver::Value::Date(_) => {
                let d = NaiveDate::try_from(self.0).unwrap();
                d.into_py(py)
            }
            databend_driver::Value::Array(inner) => {
                let list = PyList::new_bound(py, inner.into_iter().map(|v| Value(v).into_py(py)));
                list.into_py(py)
            }
            databend_driver::Value::Map(inner) => {
                let dict = PyDict::new_bound(py);
                for (k, v) in inner {
                    dict.set_item(Value(k).into_py(py), Value(v).into_py(py))
                        .unwrap();
                }
                dict.into_py(py)
            }
            databend_driver::Value::Tuple(inner) => {
                let tuple = PyTuple::new_bound(py, inner.into_iter().map(|v| Value(v).into_py(py)));
                tuple.into_py(py)
            }
            databend_driver::Value::Bitmap(s) => s.into_py(py),
            databend_driver::Value::Variant(s) => s.into_py(py),
            databend_driver::Value::Geometry(s) => s.into_py(py),
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
                let dec_cls = get_decimal_cls(py).expect("failed to load decimal.Decimal");
                let ret = dec_cls
                    .call1((self.0.to_string(),))
                    .expect("failed to call decimal.Decimal(value)");
                ret.to_object(py)
            }
            databend_driver::NumberValue::Decimal256(_, _) => {
                let dec_cls = get_decimal_cls(py).expect("failed to load decimal.Decimal");
                let ret = dec_cls
                    .call1((self.0.to_string(),))
                    .expect("failed to call decimal.Decimal(value)");
                ret.to_object(py)
            }
        }
    }
}

#[pyclass(module = "databend_driver")]
pub struct Row(databend_driver::Row);

impl Row {
    pub fn new(row: databend_driver::Row) -> Self {
        Row(row)
    }
}

#[pymethods]
impl Row {
    pub fn values<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyTuple>> {
        let vals = self.0.values().iter().map(|v| Value(v.clone()).into_py(py));
        Ok(PyTuple::new_bound(py, vals))
    }
}

#[pyclass(module = "databend_driver")]
pub struct RowIterator(Arc<Mutex<databend_driver::RowIterator>>);

impl RowIterator {
    pub fn new(streamer: databend_driver::RowIterator) -> Self {
        RowIterator(Arc::new(Mutex::new(streamer)))
    }
}

#[pymethods]
impl RowIterator {
    pub fn schema(&self, py: Python) -> PyResult<Schema> {
        let streamer = self.0.clone();
        let ret = wait_for_future(py, async move { streamer.lock().await.schema() });
        Ok(Schema(ret))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python) -> PyResult<Row> {
        let streamer = self.0.clone();
        wait_for_future(py, async move {
            match streamer.lock().await.next().await {
                Some(val) => match val {
                    Err(e) => Err(PyException::new_err(format!("{}", e))),
                    Ok(ret) => Ok(Row(ret)),
                },
                None => Err(PyStopIteration::new_err("The iterator is exhausted")),
            }
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let streamer = self.0.clone();
        future_into_py(py, async move {
            match streamer.lock().await.next().await {
                Some(val) => match val {
                    Err(e) => Err(PyException::new_err(format!("{}", e))),
                    Ok(ret) => Ok(Row(ret)),
                },
                None => Err(PyStopAsyncIteration::new_err("The iterator is exhausted")),
            }
        })
    }
}

#[pyclass(module = "databend_driver")]
pub struct Schema(databend_driver::SchemaRef);

#[pymethods]
impl Schema {
    pub fn fields<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyList>> {
        let fields = self.0.fields().iter().map(|f| Field(f.clone()).into_py(py));
        Ok(PyList::new_bound(py, fields))
    }
}

#[pyclass(module = "databend_driver")]
pub struct Field(databend_driver::Field);

#[pymethods]
impl Field {
    #[getter]
    pub fn name(&self) -> String {
        self.0.name.to_string()
    }
    #[getter]
    pub fn data_type(&self) -> String {
        self.0.data_type.to_string()
    }
}

#[pyclass(module = "databend_driver")]
pub struct ConnectionInfo(databend_driver::ConnectionInfo);

impl ConnectionInfo {
    pub fn new(info: databend_driver::ConnectionInfo) -> Self {
        ConnectionInfo(info)
    }
}

#[pymethods]
impl ConnectionInfo {
    #[getter]
    pub fn handler(&self) -> String {
        self.0.handler.to_string()
    }
    #[getter]
    pub fn host(&self) -> String {
        self.0.host.to_string()
    }
    #[getter]
    pub fn port(&self) -> u16 {
        self.0.port
    }
    #[getter]
    pub fn user(&self) -> String {
        self.0.user.to_string()
    }
    #[getter]
    pub fn database(&self) -> Option<String> {
        self.0.database.clone()
    }
    #[getter]
    pub fn warehouse(&self) -> Option<String> {
        self.0.warehouse.clone()
    }
}

#[pyclass(module = "databend_driver")]
pub struct ServerStats(databend_driver::ServerStats);

impl ServerStats {
    pub fn new(stats: databend_driver::ServerStats) -> Self {
        ServerStats(stats)
    }
}

#[pymethods]
impl ServerStats {
    #[getter]
    pub fn total_rows(&self) -> usize {
        self.0.total_rows
    }
    #[getter]
    pub fn total_bytes(&self) -> usize {
        self.0.total_bytes
    }
    #[getter]
    pub fn read_rows(&self) -> usize {
        self.0.read_rows
    }
    #[getter]
    pub fn read_bytes(&self) -> usize {
        self.0.read_bytes
    }
    #[getter]
    pub fn write_rows(&self) -> usize {
        self.0.write_rows
    }
    #[getter]
    pub fn write_bytes(&self) -> usize {
        self.0.write_bytes
    }
    #[getter]
    pub fn running_time_ms(&self) -> f64 {
        self.0.running_time_ms
    }
}

pub struct DriverError(databend_driver::Error);

impl DriverError {
    pub fn new(e: databend_driver::Error) -> Self {
        DriverError(e)
    }
}

impl From<DriverError> for PyErr {
    fn from(e: DriverError) -> Self {
        PyException::new_err(format!("{}", e.0))
    }
}
