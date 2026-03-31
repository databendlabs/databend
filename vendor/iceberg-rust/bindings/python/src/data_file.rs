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

use std::collections::HashMap;

use iceberg::spec::{DataFile, DataFileFormat, PrimitiveLiteral};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass()]
pub struct PyPrimitiveLiteral {
    inner: PrimitiveLiteral,
}

#[pymethods]
impl PyPrimitiveLiteral {
    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.inner {
            PrimitiveLiteral::Boolean(v) => v.into_py_any(py),
            PrimitiveLiteral::Int(v) => v.into_py_any(py),
            PrimitiveLiteral::Long(v) => v.into_py_any(py),
            PrimitiveLiteral::Float(v) => v.0.into_py_any(py), // unwrap OrderedFloat
            PrimitiveLiteral::Double(v) => v.0.into_py_any(py),
            PrimitiveLiteral::String(v) => v.into_py_any(py),
            PrimitiveLiteral::Binary(v) => PyBytes::new(py, v).into_py_any(py),
            PrimitiveLiteral::Int128(v) => v.into_py_any(py), // Python handles big ints
            PrimitiveLiteral::UInt128(v) => v.into_py_any(py),
            PrimitiveLiteral::AboveMax => Err(PyValueError::new_err("AboveMax is not supported")),
            PrimitiveLiteral::BelowMin => Err(PyValueError::new_err("BelowMin is not supported")),
        }
    }
}

#[pyclass]
pub struct PyDataFile {
    inner: DataFile,
}

#[pymethods]
impl PyDataFile {
    #[getter]
    fn content(&self) -> i32 {
        self.inner.content_type() as i32
    }

    #[getter]
    fn file_path(&self) -> &str {
        self.inner.file_path()
    }

    #[getter]
    fn file_format(&self) -> &str {
        match self.inner.file_format() {
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Parquet => "parquet",
            DataFileFormat::Puffin => "puffin",
        }
    }

    #[getter]
    fn partition(&self) -> Vec<Option<PyPrimitiveLiteral>> {
        self.inner
            .partition()
            .iter()
            .map(|lit| {
                lit.and_then(|l| {
                    Some(PyPrimitiveLiteral {
                        inner: l.as_primitive_literal()?,
                    })
                })
            })
            .collect()
    }

    #[getter]
    fn record_count(&self) -> u64 {
        self.inner.record_count()
    }

    #[getter]
    fn file_size_in_bytes(&self) -> u64 {
        self.inner.file_size_in_bytes()
    }

    #[getter]
    fn column_sizes(&self) -> &HashMap<i32, u64> {
        self.inner.column_sizes()
    }

    #[getter]
    fn value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.value_counts()
    }

    #[getter]
    fn null_value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.null_value_counts()
    }

    #[getter]
    fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.nan_value_counts()
    }

    #[getter]
    fn upper_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.inner
            .upper_bounds()
            .iter()
            .map(|(k, v)| (*k, v.to_bytes().unwrap().to_vec()))
            .collect()
    }

    #[getter]
    fn lower_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.inner
            .lower_bounds()
            .iter()
            .map(|(k, v)| (*k, v.to_bytes().unwrap().to_vec()))
            .collect()
    }

    #[getter]
    fn key_metadata(&self) -> Option<&[u8]> {
        self.inner.key_metadata()
    }

    #[getter]
    fn split_offsets(&self) -> Option<&[i64]> {
        self.inner.split_offsets()
    }

    #[getter]
    fn equality_ids(&self) -> Option<Vec<i32>> {
        self.inner.equality_ids()
    }

    #[getter]
    fn sort_order_id(&self) -> Option<i32> {
        self.inner.sort_order_id()
    }
}

impl PyDataFile {
    pub fn new(inner: DataFile) -> Self {
        Self { inner }
    }
}
