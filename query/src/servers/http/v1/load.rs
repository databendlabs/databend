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
use common_meta_types::UserInfo;
use common_planners::InsertInputSource;
use common_planners::PlanNode;
use common_streams::CsvSource;
use common_streams::Source;
use common_tracing::tracing;
use futures::StreamExt;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
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
    user_info: Data<&UserInfo>,
    sessions_extension: Data<&Arc<SessionManager>>,
) -> PoemResult<Json<LoadResponse>> {
    let session_manager = sessions_extension.0;
    let session = session_manager
        .create_session("Streaming load")
        .map_err(InternalServerError)?;

    // TODO: list user's grant list and check client address

    session.set_current_user(user_info.0.clone());

    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;
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

    let field_delimitor = req
        .headers()
        .get("field_delimitor")
        .and_then(|v| v.to_str().ok())
        .map(|v| match v.len() {
            n if n >= 1 => {
                if v.as_bytes()[0] == b'\\' {
                    b'\t'
                } else {
                    v.as_bytes()[0]
                }
            }
            _ => b',',
        })
        .unwrap_or(b',');

    let record_delimitor = req
        .headers()
        .get("record_delimitor")
        .and_then(|v| v.to_str().ok())
        .map(|v| match v.len() {
            n if n >= 1 => {
                if v.as_bytes()[0] == b'\\' {
                    b'\n'
                } else {
                    v.as_bytes()[0]
                }
            }
            _ => b'\n',
        })
        .unwrap_or(b'\n');

    let plan = PlanParser::parse(context.clone(), insert_sql)
        .await
        .map_err(InternalServerError)?;
    context.attach_query_str(insert_sql);

    // validate plan
    match &plan {
        PlanNode::Insert(insert) => match &insert.source {
            InsertInputSource::StreamingWithFormat(format) => {
                if format.to_lowercase().as_str() == "csv" {
                    Ok(())
                } else {
                    Err(poem::Error::from_string(
                        format!(
                            "Streaming load only supports csv format, but got {}",
                            format
                        ),
                        StatusCode::BAD_REQUEST,
                    ))
                }
            }
            _non_supported_source => Err(poem::Error::from_string(
                "Only supports streaming upload. e.g. INSERT INTO $table FORMAT CSV",
                StatusCode::BAD_REQUEST,
            )),
        },
        non_insert_plan => Err(poem::Error::from_string(
            format!(
                "Only supports INSERT statement in streaming load, but got {}",
                non_insert_plan.name()
            ),
            StatusCode::BAD_REQUEST,
        )),
    }?;

    let max_block_size = context
        .get_settings()
        .get_max_block_size()
        .map_err(InternalServerError)? as usize;
    let interpreter =
        InterpreterFactory::get(context.clone(), plan.clone()).map_err(InternalServerError)?;
    // Write Start to query log table.
    let _ = interpreter
        .start()
        .await
        .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

    let stream = stream! {
        while let Ok(Some(field)) = multipart.next_field().await {
            let reader = field.into_async_read();
            let mut source = CsvSource::try_create(reader.compat(), plan.schema(), csv_header, field_delimitor, record_delimitor, max_block_size)?;

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
    let mut data_stream = interpreter
        .execute(Some(Box::pin(stream)))
        .await
        .map_err(InternalServerError)?;
    while let Some(_block) = data_stream.next().await {}

    // Write Finish to query log table.
    let _ = interpreter
        .finish()
        .await
        .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));

    // TODO generate id
    // TODO duplicate by insert_label
    let mut id = uuid::Uuid::new_v4().to_string();
    Ok(Json(LoadResponse {
        id,
        state: "SUCCESS".to_string(),
        stats: context.get_scan_progress_value(),
        error: None,
    }))
}
