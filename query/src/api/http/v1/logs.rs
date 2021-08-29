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
use common_planners::PlanNode;
use futures::TryStreamExt;
use warp::reject::Reject;
use warp::Filter;

use crate::interpreters::SelectInterpreter;
use crate::sessions::SessionManager;
use crate::sql::PlanParser;

pub fn log_handler() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "logs").and(warp::get()).and_then(get_log)
}

async fn get_log() -> Result<impl warp::Reply, warp::Rejection> {
    let result = select_table().await;
    match result {
        Ok(s) => Ok(warp::reply::with_status(
            s.to_string(),
            warp::http::StatusCode::OK,
        )),
        Err(error_codes) => Err(warp::reject::custom(NoBacktraceErrorCode(error_codes))),
    }
}

async fn select_table() -> Result<String, ErrorCode> {
    let session_manager = SessionManager::try_create(1)?;
    let executor_session = session_manager.create_session("HTTP")?;
    let ctx = executor_session.create_context();
    if let PlanNode::Select(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("select * from system.tracing")?
    {
        /*
        let table_meta = ctx.get_table("system", "tracing")?;
        let table = table_meta.datasource();
        let source_plan = table.read_plan(ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_settings().get_max_threads()? as usize);
        */
        let executor = SelectInterpreter::try_create(ctx.clone(), plan.clone())?;
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let m = result[0].column_by_name("*");
        match m {
            Some(s1) => {
                let r = format!("{:?}", s1);
                return Ok(r.to_string());
            }
            None => todo!(),
        }
        
        //let r = format!("{:?}", result.as_slice());
        //return Ok(r.to_string());
    }
    Ok("".to_string())
}
struct NoBacktraceErrorCode(ErrorCode);

impl Debug for NoBacktraceErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Reject for NoBacktraceErrorCode {}
