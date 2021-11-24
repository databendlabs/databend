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

use common_datablocks::DataBlock;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use poem::http::StatusCode;
use poem::web::Data;
use poem::IntoResponse;
use tokio_stream::StreamExt;

use crate::catalogs::ToReadDataSourcePlan;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::SessionManagerRef;

// read log files from cfg.log.log_dir
#[poem::handler]
pub async fn logs_handler(
    sessions_extension: Data<&SessionManagerRef>,
) -> poem::Result<impl IntoResponse> {
    let data = select_table(sessions_extension.0).await.map_err(|err| {
        poem::Error::new(StatusCode::INTERNAL_SERVER_ERROR)
            .with_reason(format!("Failed to fetch log. Error: {}", err))
    })?;
    Ok(data)
}

async fn select_table(sessions: &SessionManagerRef) -> Result<String> {
    let session = sessions.create_session("WatchLogs")?;
    let query_context = session.create_context().await?;

    let tracing_table_stream = execute_query(query_context).await?;
    let tracing_logs = tracing_table_stream
        .collect::<Result<Vec<DataBlock>>>()
        .await?;
    Ok(format!("{:?}", tracing_logs))
}

async fn execute_query(ctx: DatabendQueryContextRef) -> Result<SendableDataBlockStream> {
    let tracing_table = ctx.get_table("system", "tracing").await?;
    let tracing_table_read_plan = tracing_table.read_plan(ctx.clone(), None).await?;

    tracing_table.read(ctx, &tracing_table_read_plan).await
}
