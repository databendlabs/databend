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

#![feature(try_blocks)]

mod datablock;
mod utils;

use std::sync::Arc;

use common_config::InnerConfig;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_app::principal::UserInfo;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use databend_query::GlobalServices;
use datablock::Block;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use utils::get_ctx;
use utils::wait_for_future;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(Runtime);

#[pyclass]
pub(crate) struct DatabendCtx(Arc<QueryContext>);

#[pyfunction]
fn sql(sql: &str, py: Python) -> PyResult<Block> {
    let ctx = get_ctx(py);
    let ctx = ctx.0.clone();
    let res: Result<Block> = wait_for_future(py, query_local(ctx, sql));
    Ok(res.unwrap())
}

pub async fn query_local(ctx: Arc<QueryContext>, sql: &str) -> Result<Block> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    let blocks = stream.map(|v| v.unwrap()).collect::<Vec<_>>().await;
    let block = if !blocks.is_empty() {
        DataBlock::concat(&blocks)
    } else {
        Ok(DataBlock::empty_with_schema(plan.schema()))
    }?;
    Ok(Block(block))
}

/// A Python module implemented in Rust.
#[pymodule]
fn databend(_py: Python, m: &PyModule) -> PyResult<()> {
    let runtime = Runtime::new().unwrap();

    runtime.block_on(async {
        let mut conf: InnerConfig = InnerConfig::default();
        conf.storage.allow_insecure = true;
        GlobalServices::init(conf).await.unwrap();
    });

    let ctx = runtime.block_on(async {
        let session = SessionManager::instance()
            .create_session(SessionType::Local)
            .await
            .unwrap();

        let user = UserInfo::new_no_auth("root", "127.0.0.1");
        session.set_authed_user(user, None).await.unwrap();
        session.create_query_context().await.unwrap()
    });

    m.add("runtime", TokioRuntime(Runtime::new().unwrap()))?;
    m.add("ctx", DatabendCtx(ctx))?;

    m.add_function(wrap_pyfunction!(sql, m)?)?;

    Ok(())
}
