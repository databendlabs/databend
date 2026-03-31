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
use std::ffi::CString;
use std::sync::Arc;

use datafusion_ffi::table_provider::FFI_TableProvider;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg_datafusion::table::IcebergStaticTableProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::runtime::runtime;

#[pyclass(name = "IcebergDataFusionTable")]
pub struct PyIcebergDataFusionTable {
    inner: Arc<IcebergStaticTableProvider>,
}

#[pymethods]
impl PyIcebergDataFusionTable {
    #[new]
    fn new(
        identifier: Vec<String>,
        metadata_location: String,
        file_io_properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let runtime = runtime();

        let provider = runtime.block_on(async {
            let table_ident = TableIdent::from_strs(identifier)
                .map_err(|e| PyRuntimeError::new_err(format!("Invalid table identifier: {e}")))?;

            let mut builder = FileIO::from_path(&metadata_location)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to init FileIO: {e}")))?;

            if let Some(props) = file_io_properties {
                builder = builder.with_props(props);
            }

            let file_io = builder
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to build FileIO: {e}")))?;

            let static_table =
                StaticTable::from_metadata_file(&metadata_location, table_ident, file_io)
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to load static table: {e}"))
                    })?;

            let table = static_table.into_table();

            IcebergStaticTableProvider::try_new_from_table(table)
                .await
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create table provider: {e}"))
                })
        })?;

        Ok(Self {
            inner: Arc::new(provider),
        })
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let capsule_name = CString::new("datafusion_table_provider").unwrap();

        let ffi_provider = FFI_TableProvider::new(self.inner.clone(), false, Some(runtime()));

        PyCapsule::new(py, ffi_provider, Some(capsule_name))
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "datafusion")?;

    this.add_class::<PyIcebergDataFusionTable>()?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.datafusion", this)?;

    Ok(())
}
