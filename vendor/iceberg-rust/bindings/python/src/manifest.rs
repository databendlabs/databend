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

use std::sync::Arc;

use iceberg::spec::{
    FieldSummary, FormatVersion, Manifest, ManifestEntry, ManifestFile, ManifestList,
};
use pyo3::prelude::*;

use crate::data_file::PyDataFile;

#[pyclass]
pub struct PyManifest {
    inner: Manifest,
}

#[pymethods]
impl PyManifest {
    fn entries(&self) -> Vec<PyManifestEntry> {
        // TODO: Most of the time, we're only interested in 'alive' entries,
        // that are the ones that are either ADDED or EXISTING
        // so we can add a boolean to skip the DELETED entries right away before
        // moving it into the Python world
        self.inner
            .entries()
            .iter()
            .map(|entry| PyManifestEntry {
                inner: entry.clone(),
            })
            .collect()
    }
}

#[pyclass]
pub struct PyFieldSummary {
    inner: FieldSummary,
}

#[pymethods]
impl crate::manifest::PyFieldSummary {
    #[getter]
    fn contains_null(&self) -> bool {
        self.inner.contains_null
    }

    #[getter]
    fn contains_nan(&self) -> Option<bool> {
        self.inner.contains_nan
    }

    #[getter]
    fn lower_bound(&self) -> Option<Vec<u8>> {
        self.inner.lower_bound.clone().map(|b| b.to_vec())
    }

    #[getter]
    fn upper_bound(&self) -> Option<Vec<u8>> {
        self.inner.upper_bound.clone().map(|b| b.to_vec())
    }
}

#[pyclass]
pub struct PyManifestFile {
    inner: ManifestFile,
}

#[pymethods]
impl crate::manifest::PyManifestFile {
    #[getter]
    fn manifest_path(&self) -> &str {
        self.inner.manifest_path.as_str()
    }
    #[getter]
    fn manifest_length(&self) -> i64 {
        self.inner.manifest_length
    }
    #[getter]
    fn partition_spec_id(&self) -> i32 {
        self.inner.partition_spec_id
    }

    #[getter]
    fn content(&self) -> i32 {
        self.inner.content as i32
    }

    #[getter]
    fn sequence_number(&self) -> i64 {
        self.inner.sequence_number
    }

    #[getter]
    fn min_sequence_number(&self) -> i64 {
        self.inner.min_sequence_number
    }

    #[getter]
    fn added_snapshot_id(&self) -> i64 {
        self.inner.added_snapshot_id
    }

    #[getter]
    fn added_files_count(&self) -> Option<u32> {
        self.inner.added_files_count
    }

    #[getter]
    fn existing_files_count(&self) -> Option<u32> {
        self.inner.existing_files_count
    }

    #[getter]
    fn deleted_files_count(&self) -> Option<u32> {
        self.inner.deleted_files_count
    }

    #[getter]
    fn added_rows_count(&self) -> Option<u64> {
        self.inner.added_rows_count
    }

    #[getter]
    fn existing_rows_count(&self) -> Option<u64> {
        self.inner.existing_rows_count
    }

    #[getter]
    fn deleted_rows_count(&self) -> Option<u64> {
        self.inner.deleted_rows_count
    }

    #[getter]
    fn partitions(&self) -> Vec<PyFieldSummary> {
        self.inner
            .partitions
            .clone()
            .unwrap()
            .iter()
            .map(|s| PyFieldSummary { inner: s.clone() })
            .collect()
    }

    #[getter]
    fn key_metadata(&self) -> Option<Vec<u8>> {
        self.inner.key_metadata.clone()
    }
}

#[pyclass]
pub struct PyManifestEntry {
    inner: Arc<ManifestEntry>,
}

#[pymethods]
impl PyManifestEntry {
    #[getter]
    fn status(&self) -> i32 {
        self.inner.status as i32
    }

    #[getter]
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id
    }

    #[getter]
    fn sequence_number(&self) -> Option<i64> {
        self.inner.sequence_number
    }

    #[getter]
    fn file_sequence_number(&self) -> Option<i64> {
        self.inner.file_sequence_number
    }

    #[getter]
    fn data_file(&self) -> PyDataFile {
        PyDataFile::new(self.inner.data_file.clone())
    }
}

#[pyfunction]
pub fn read_manifest_entries(bs: &[u8]) -> PyManifest {
    // TODO: Some error handling
    PyManifest {
        inner: Manifest::parse_avro(bs).unwrap(),
    }
}

#[pyclass]
pub struct PyManifestList {
    inner: ManifestList,
}

#[pymethods]
impl crate::manifest::PyManifestList {
    fn entries(&self) -> Vec<PyManifestFile> {
        self.inner
            .entries()
            .iter()
            .map(|file| PyManifestFile {
                inner: file.clone(),
            })
            .collect()
    }
}

#[pyfunction]
pub fn read_manifest_list(bs: &[u8]) -> PyManifestList {
    PyManifestList {
        inner: ManifestList::parse_with_version(bs, FormatVersion::V2).unwrap(),
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "manifest")?;

    this.add_function(wrap_pyfunction!(read_manifest_entries, &this)?)?;
    this.add_function(wrap_pyfunction!(read_manifest_list, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.manifest", this)
}
