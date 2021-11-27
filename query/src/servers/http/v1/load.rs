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

use async_compat::CompatExt;
use async_stream::stream;
use common_base::ProgressValues;
use common_planners::PlanNode;
use common_streams::CsvSource;
use common_streams::Source;
use futures::StreamExt;
use poem::error::BadRequest;
use poem::error::Result as PoemResult;
use poem::web::Data;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::InterpreterFactory;
use crate::sessions::SessionManager;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub state: String,
    pub stats: ProgressValues,
    pub error: Option<String>,
}

#[poem::handler]
pub async fn streaming_load(
    req: &Request,
    mut multipart: Multipart,
    sessions_extension: Data<&Arc<SessionManager>>,
) -> PoemResult<Json<LoadResponse>> {
    let session_manager = sessions_extension.0;
    let session = session_manager.create_session("Streaming load")?;
    let context = session.create_context().await?;
    let insert_sql = req
        .headers()
        .get("insert_sql")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let csv_header = req
        .headers()
        .get("csv_header")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("0")
        .eq_ignore_ascii_case("1");

    let plan = PlanParser::parse(insert_sql, context.clone()).await?;
    let max_block_size = context.get_settings().get_max_block_size()? as usize;
    let interpreter = InterpreterFactory::get(context.clone(), plan.clone())?;

    context.attach_query_str(insert_sql);
    if let PlanNode::InsertInto(insert) = &plan {
        if insert.format.is_none() {
            return Err(BadRequest(
                "Streaming load only support csv format, but got none",
            ));
        }

        if insert.format.as_ref().unwrap().to_lowercase().as_str() != "csv" {
            return Err(BadRequest("Streaming load only support csv format"));
        }
    }

    let stream = stream! {
        while let Ok(Some(field)) = multipart.next_field().await {
            let reader = field.into_async_read();
            let mut source = CsvSource::try_create(reader.compat(), plan.schema(), csv_header, max_block_size)?;

            loop {
                let block = source.read().await;
                match block {
                    Ok(None) => break,
                    Ok(Some(b)) =>  yield(Ok(b)),
                    Err(e) => yield(Err(e)),
                }
            }
        }
    };

    // this runs inside the runtime of poem, load is not cpu densive so it's ok
    let mut data_stream = interpreter.execute(Some(Box::pin(stream))).await?;
    while let Some(_block) = data_stream.next().await {}

    // TODO generate id
    // TODO duplicate by insert_label
    let mut id = uuid::Uuid::new_v4().to_string();
    Ok(Json(LoadResponse {
        id,
        state: "SUCCESS".to_string(),
        stats: context.get_progress_value(),
        error: None,
    }))
}
