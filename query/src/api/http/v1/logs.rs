// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use poem::http::StatusCode;
use poem::web::Data;
use poem::Body;
use poem::IntoResponse;
use tokio_stream::StreamExt;

use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::storages::ToReadDataSourcePlan;

// read log files from cfg.log.log_dir
#[poem::handler]
pub async fn logs_handler(
    sessions_extension: Data<&Arc<SessionManager>>,
) -> poem::Result<impl IntoResponse> {
    let data = select_table(sessions_extension.0).await.map_err(|err| {
        poem::Error::from_string(
            format!("Failed to fetch log. Error: {err}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(data)
}

async fn select_table(sessions: &Arc<SessionManager>) -> Result<Body> {
    let session = sessions.create_session("WatchLogs")?;
    let query_context = session.create_query_context().await?;
    let mut tracing_table_stream = execute_query(query_context).await?;

    let stream = async_stream::try_stream! {
        match tracing_table_stream.next().await {
            Some(res) => {
                let block = res?;
                yield format!("{block:?}");
            },
            None => return,
        }

        while let Some(res) = tracing_table_stream.next().await {
            let block = res?;
            yield format!(", {block:?}");
        }
    };

    Ok(Body::from_bytes_stream::<_, _, ErrorCode>(stream))
}

async fn execute_query(ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
    let tracing_table = ctx.get_table("system", "tracing").await?;
    let tracing_table_read_plan = tracing_table.read_plan(ctx.clone(), None).await?;

    tracing_table.read(ctx, &tracing_table_read_plan).await
}
