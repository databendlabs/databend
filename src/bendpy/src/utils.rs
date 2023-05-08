use std::future::Future;
use std::sync::Arc;

use databend_query::sessions::QueryContext;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::DatabendCtx;
use crate::TokioRuntime;

/// Utility to get the Tokio Runtime from Python
pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let databend = py.import("databend").unwrap();
    databend.getattr("runtime").unwrap().extract().unwrap()
}

pub(crate) fn get_ctx(py: Python) -> PyRef<DatabendCtx> {
    let databend = py.import("databend").unwrap();
    databend.getattr("ctx").unwrap().extract().unwrap()
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}
