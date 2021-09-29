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
use common_datablocks::DataBlock;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::sessions::DatabendQueryContextRef;
use crate::sessions::SessionManagerRef;

pub struct LogTemplate {
    result: Result<String>,
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
pub async fn logs_handler(sessions_extension: Extension<SessionManagerRef>) -> LogTemplate {
    let sessions = sessions_extension.0;
    LogTemplate {
        result: select_table(sessions).await,
    }
}

async fn select_table(sessions: SessionManagerRef) -> Result<String> {
    let session = sessions.create_session("WatchLogs")?;
    let query_context = session.create_context().await?;

    let tracing_table_stream = execute_query(query_context).await?;
    let tracing_logs = tracing_table_stream
        .collect::<Result<Vec<DataBlock>>>()
        .await?;
    Ok(format!("{:?}", tracing_logs))
}

async fn execute_query(context: DatabendQueryContextRef) -> Result<SendableDataBlockStream> {
    let tracing_table_meta = context.get_table("system", "tracing")?;

    let tracing_table = tracing_table_meta.raw();
    let tracing_table_read_plan = tracing_table.read_plan(
        context.clone(),
        None,
        Some(context.get_settings().get_max_threads()? as usize),
    )?;

    tracing_table
        .read(context.clone(), &tracing_table_read_plan)
        .await
}
