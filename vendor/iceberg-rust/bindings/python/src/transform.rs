// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, ArrayData, make_array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use iceberg::spec::Transform;
use iceberg::transform::create_transform_function;
use pyo3::prelude::*;

use crate::error::to_py_err;

#[pyfunction]
pub fn identity(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Identity)
}

#[pyfunction]
pub fn void(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Void)
}

#[pyfunction]
pub fn year(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Year)
}

#[pyfunction]
pub fn month(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Month)
}

#[pyfunction]
pub fn day(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Day)
}

#[pyfunction]
pub fn hour(py: Python, array: Py<PyAny>) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Hour)
}

#[pyfunction]
pub fn bucket(py: Python, array: Py<PyAny>, num_buckets: u32) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Bucket(num_buckets))
}

#[pyfunction]
pub fn truncate(py: Python, array: Py<PyAny>, width: u32) -> PyResult<Py<PyAny>> {
    apply(py, array, Transform::Truncate(width))
}

fn apply(py: Python, array: Py<PyAny>, transform: Transform) -> PyResult<Py<PyAny>> {
    // import
    let array = ArrayData::from_pyarrow_bound(array.bind(py))?;
    let array = make_array(array);
    let transform_function = create_transform_function(&transform).map_err(to_py_err)?;
    let array = transform_function.transform(array).map_err(to_py_err)?;
    // export
    let array = array.into_data();
    Ok(array.to_pyarrow(py)?.unbind())
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "transform")?;

    this.add_function(wrap_pyfunction!(identity, &this)?)?;
    this.add_function(wrap_pyfunction!(void, &this)?)?;
    this.add_function(wrap_pyfunction!(year, &this)?)?;
    this.add_function(wrap_pyfunction!(month, &this)?)?;
    this.add_function(wrap_pyfunction!(day, &this)?)?;
    this.add_function(wrap_pyfunction!(hour, &this)?)?;
    this.add_function(wrap_pyfunction!(bucket, &this)?)?;
    this.add_function(wrap_pyfunction!(truncate, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.transform", this)
}
