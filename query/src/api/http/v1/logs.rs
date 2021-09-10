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

use std::convert::Infallible;

use axum::body::Bytes;
use axum::body::Full;
use axum::extract::Extension;
use axum::http::Response;
use axum::http::StatusCode;
use axum::response::Html;
use axum::response::IntoResponse;
use common_exception::ErrorCode;
use common_planners::ScanPlan;
use futures::TryStreamExt;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::SessionManager;

pub struct LogTemplate {
    result: Result<String, ErrorCode>,
}
impl IntoResponse for LogTemplate {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        match self.result {
            Ok(log) => Html(log).into_response(),
            Err(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::from(format!("Failed to fetch log. Error: {}", err)))
                .unwrap(),
        }
    }
}

// read log files from cfg.log.log_dir
pub async fn logs_handler(cfg_extension: Extension<Config>) -> LogTemplate {
    let cfg = cfg_extension.0;
    log::info!(
        "Read logs from : {} with log level {}",
        cfg.log.log_dir,
        cfg.log.log_level
    );
    LogTemplate {
        result: select_table(cfg).await,
    }
}

async fn select_table(cfg: Config) -> Result<String, ErrorCode> {
    let session_manager = SessionManager::from_conf(cfg, Cluster::empty().await)?;
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
    Ok(r)
}
