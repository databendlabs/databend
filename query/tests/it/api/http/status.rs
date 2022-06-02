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

use common_base::base::tokio;
use common_exception::Result;
use common_meta_types::UserIdentity;
use databend_query::api::http::v1::status::status_handler;
use databend_query::api::http::v1::status::Status;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::PlanParser;
use poem::get;
use poem::http::header;
use poem::http::Method;
use poem::http::StatusCode;
use poem::http::Uri;
use poem::middleware::AddDataEndpoint;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::tests::SessionManagerBuilder;

async fn get_status(ep: &AddDataEndpoint<Route, Arc<SessionManager>>) -> Status {
    let response = ep
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/status"))
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_vec().await.unwrap();
    serde_json::from_str::<Status>(&String::from_utf8_lossy(&body)).unwrap()
}

async fn run_query(sessions: Arc<SessionManager>) -> Result<Arc<dyn Interpreter>> {
    let sql = "select * from numbers(1)";
    let session = sessions.create_session(SessionType::HTTPQuery).await?;
    let ctx = session.create_query_context().await?;
    ctx.attach_query_str(sql);
    let user = ctx
        .get_user_manager()
        .get_user("test", UserIdentity::new("root", "localhost"))
        .await?;
    session.set_current_user(user);
    let plan = PlanParser::parse(ctx.clone(), sql).await?;
    InterpreterFactory::get(ctx.clone(), plan)
}

#[tokio::test]
async fn test_status() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let ep = Route::new()
        .at("/v1/status", get(status_handler))
        .data(sessions.clone());

    let status = get_status(&ep).await;
    assert_eq!(
        (
            status.running_queries_count,
            status.last_query_started_at.is_some(),
            status.last_query_finished_at.is_some(),
        ),
        (0, false, false),
        "before running"
    );

    let interpreter = run_query(sessions.clone()).await?;
    interpreter.start().await?;
    let status = get_status(&ep).await;
    assert_eq!(
        (
            status.running_queries_count,
            status.last_query_started_at.is_some(),
            status.last_query_finished_at.is_some(),
        ),
        (1, true, false),
        "running"
    );
    interpreter.finish().await?;
    let status = get_status(&ep).await;
    assert_eq!(
        (
            status.running_queries_count,
            status.last_query_started_at.is_some(),
            status.last_query_finished_at.is_some(),
        ),
        (0, true, true),
        "finished"
    );

    Ok(())
}
