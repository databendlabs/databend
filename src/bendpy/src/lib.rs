#![feature(try_blocks)]

mod utils;

use std::fmt::Write;

use common_config::InnerConfig;
use common_exception::Result;
use common_expression::block_debug::pretty_format_blocks;
use common_meta_app::principal::UserInfo;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use utils::wait_for_future;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(Runtime);

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn sql(sql: &str, py: Python) -> PyResult<String> {
    let res: Result<String> = wait_for_future(py, query_local(sql));
    Ok(res.unwrap())
}

pub async fn query_local(sql: &str) -> Result<String> {
    let mut conf: InnerConfig = InnerConfig::default();
    conf.storage.allow_insecure = true;
    GlobalServices::init(conf).await?;

    let session = SessionManager::instance()
        .create_session(SessionType::Local)
        .await?;

    let user = UserInfo::new_no_auth("root", "127.0.0.1");
    session.set_authed_user(user, None).await?;
    let ctx = session.create_query_context().await?;

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    let blocks = stream.map(|v| v.unwrap()).collect::<Vec<_>>().await;
    pretty_format_blocks(&blocks)
}

/// A Python module implemented in Rust.
#[pymodule]
fn databend(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("runtime", TokioRuntime(Runtime::new().unwrap()))?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(sql, m)?)?;

    Ok(())
}
