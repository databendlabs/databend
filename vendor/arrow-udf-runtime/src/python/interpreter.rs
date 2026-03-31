// Copyright 2024 RisingWave Labs
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

//! High-level API for Python interpreters.
//!
//! # Notes
//!
//! Previously, we used `PyO3` to create a new Python sub-interpreter. However,
//! due to limitations in the new `PyO3` API, we can no longer implement it as we
//! did before.
//! For now, we are deprecating this module but forwarding the `PyO3` API to the
//! new one to ensure a smoother migration path.
//! You may choose to refactor this module to use `PyO3` directly at a later time.

use std::{ffi::CString, sync::Once};

use pyo3::{PyErr, Python};

/// A Python interpreter with its own GIL.
#[derive(Debug)]
pub struct Interpreter;

static GLOBAL_INIT: Once = Once::new();

impl Interpreter {
    /// Create a new sub-interpreter.
    pub fn new() -> Result<Self, PyError> {
        GLOBAL_INIT.call_once_force(|_| {
            // use call_once_force because if initialization panics, it's okay to try again

            // XXX: import the `decimal` module in the interpreter before calling anything else
            //      otherwise it will cause `SIGABRT: pointer being freed was not allocated`
            //      when importing decimal in the second sub-interpreter.
            Python::with_gil(|py| {
                py.import("decimal").unwrap();
            });
        });

        Ok(Self)
    }

    /// Run a closure in the sub-interpreter.
    ///
    /// Please note that if the return value contains any `Py` object (e.g. `PyErr`),
    /// this object must be dropped in this sub-interpreter, otherwise it will cause
    /// `SIGABRT: pointer being freed was not allocated`.
    pub fn with_gil<F, R>(&self, f: F) -> Result<R, PyError>
    where
        F: for<'py> FnOnce(Python<'py>) -> Result<R, PyError>,
    {
        Python::with_gil(f)
    }

    /// Run Python code in the sub-interpreter.
    pub fn run(&self, code: &str) -> Result<(), PyError> {
        let code = CString::new(code).unwrap();
        self.with_gil(|py| py.run(&code, None, None).map_err(|e| e.into()))
    }
}

/// The error type for Python interpreters.
///
/// This type is a wrapper around `anyhow::Error`. The special thing is that
/// when it comes from `PyErr`, only the error message is retained, and the
/// original type is discarded. This is to avoid the problem of `PyErr` being
/// dropped outside the interpreter.
#[derive(Debug)]
pub struct PyError {
    anyhow: anyhow::Error,
}

/// Converting from `PyErr` only keeps the error message.
impl From<PyErr> for PyError {
    fn from(err: PyErr) -> Self {
        Self {
            anyhow: anyhow::anyhow!(err.to_string()),
        }
    }
}

impl From<anyhow::Error> for PyError {
    fn from(err: anyhow::Error) -> Self {
        Self { anyhow: err }
    }
}

impl From<PyError> for anyhow::Error {
    fn from(err: PyError) -> Self {
        err.anyhow
    }
}
