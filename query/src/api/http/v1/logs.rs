// Copyright 2020 Datafuse Labs.
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
use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_planners::ScanPlan;
use futures::TryStreamExt;
use warp::reject::Reject;
use warp::Filter;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::SessionManager;

pub fn log_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "logs")
        .and(warp::get())
        .and(warp::any().map(move || cfg.clone()))
        .and_then(get_log)
}

async fn get_log(cfg: Config) -> Result<impl warp::Reply, warp::Rejection> {
    let result = select_table(cfg).await;
    match result {
        Ok(s) => Ok(warp::reply::with_status(
            s.to_string(),
            warp::http::StatusCode::OK,
        )),
        Err(error_codes) => Err(warp::reject::custom(NoBacktraceErrorCode(error_codes))),
    }
}

async fn select_table(cfg: Config) -> Result<String, ErrorCode> {
    let session_manager = SessionManager::from_conf(cfg, Cluster::empty())?;
    let executor_session = session_manager.create_session("HTTP")?;
    let ctx = executor_session.create_context();
    let table_meta = ctx.get_table("system", "tracing")?;
    let table = table_meta.raw();
    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_settings().get_max_threads()? as usize,
    )?;
    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let r = format!("{:?}", result);
    Ok(r.to_string())
}
struct NoBacktraceErrorCode(ErrorCode);

impl Debug for NoBacktraceErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Reject for NoBacktraceErrorCode {}
