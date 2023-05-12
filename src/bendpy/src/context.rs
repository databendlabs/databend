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

use common_exception::Result;
use common_meta_app::principal::UserInfo;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::utils::wait_for_future;
use crate::utils::RUNTIME;

#[pyclass(name = "SessionContext", module = "databend", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) ctx: Arc<QueryContext>,
}

#[pymethods]
impl PySessionContext {
    #[new]
    fn new() -> PyResult<Self> {
        let ctx = RUNTIME.block_on(async {
            let session = SessionManager::instance()
                .create_session(SessionType::Local)
                .await
                .unwrap();

            let user = UserInfo::new_no_auth("root", "127.0.0.1");
            session.set_authed_user(user, None).await.unwrap();
            session.create_query_context().await.unwrap()
        });

        Ok(PySessionContext { ctx })
    }

    fn sql(&mut self, sql: &str, py: Python) -> PyResult<PyDataFrame> {
        let res = wait_for_future(py, plan_sql(&self.ctx, sql));
        match res {
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error: {}",
                err
            ))),
            Ok(res) => {
                // if res.df.has_result_set() {
                //     return Ok(res);
                // } else {
                //     return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                //         "Error: sql method only supports SELECT queries",
                //     ));
                // }
                Ok(res)
            }
        }
    }
}

async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<PyDataFrame> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    Ok(PyDataFrame::new(ctx.clone(), plan))
}
