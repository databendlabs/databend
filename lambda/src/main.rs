use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use common_base::base::TrySpawn;
use common_datablocks::pretty_format_blocks;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_io::prelude::convert_byte_size;
use common_io::prelude::convert_number_size;
use common_meta_embedded::MetaEmbedded;
use common_tracing::init_global_tracing;
use common_tracing::set_panic_hook;
use common_tracing::tracing;
use common_tracing::tracing::debug;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactoryV2;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use databend_query::Config;
use lambda_http::run;
use lambda_http::service_fn;
use lambda_http::Body;
use lambda_http::Request;
use lambda_http::Response;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    MetaEmbedded::init_global_meta_store("/tmp/meta_embedded".to_string()).await?;

    let conf: Config = Config::load()?;

    let tenant = conf.query.tenant_id.clone();
    let cluster_id = conf.query.cluster_id.clone();
    let app_name = format!("databend-query-{}-{}", &tenant, &cluster_id);

    let _guards = init_global_tracing(
        app_name.as_str(),
        conf.log.dir.as_str(),
        conf.log.level.as_str(),
        None,
    );

    set_panic_hook();
    tracing::info!("{:?}", conf);
    tracing::info!("DatabendQuery {}", *databend_query::DATABEND_COMMIT_VERSION);

    run(service_fn(handle)).await
}

async fn handle(event: Request) -> Result<Response<Body>, lambda_http::Error> {
    let conf: Config = Config::load()?;

    let session_manager = SessionManager::from_conf(conf.clone()).await?;

    let (head, body) = event.into_parts();
    debug!("got request, head: {head:?}");

    let sql = match body {
        Body::Empty => return Err(anyhow!("sql is empty").into()),
        Body::Text(v) => v,
        Body::Binary(v) => String::from_utf8_lossy(&v).to_string(),
    };

    debug!("start exec sql: {sql}");

    let session = session_manager
        .create_session(SessionType::Clickhouse)
        .await?;
    let context = session.create_query_context().await?;
    context.attach_query_str(&sql);

    let mut planner = Planner::new(context.clone());
    let interpreter = planner
        .plan_sql(&sql)
        .await
        .and_then(|v| InterpreterFactoryV2::get(context.clone(), &v.0))?;

    let (data_blocks, _) = exec_query(interpreter, &context).await?;

    let resp = Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(pretty_format_blocks(&data_blocks)?.into())
        .map_err(Box::new)?;

    Ok(resp)
}

async fn exec_query(
    interpreter: Arc<dyn Interpreter>,
    context: &Arc<QueryContext>,
) -> Result<(Vec<DataBlock>, String), ErrorCode> {
    let instant = Instant::now();

    let query_result = context.try_spawn(async move {
        // Write start query log.
        let _ = interpreter
            .start()
            .await
            .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));
        let data_stream = interpreter.execute(None).await?;

        let collector = data_stream.collect::<Result<Vec<DataBlock>, ErrorCode>>();
        let query_result = collector.await?;
        // Write finish query log.
        let _ = interpreter
            .finish()
            .await
            .map_err(|e| tracing::error!("interpreter.finish.error: {:?}", e));

        Ok::<Vec<DataBlock>, ErrorCode>(query_result)
    })?;

    let query_result = query_result.await.map_err_to_code(
        ErrorCode::TokioError,
        || "Cannot join handle from context's runtime",
    )?;
    query_result.map(|data| {
        if data.is_empty() {
            (data, "".to_string())
        } else {
            (data, extra_info(context, instant))
        }
    })
}

fn extra_info(context: &Arc<QueryContext>, instant: Instant) -> String {
    let progress = context.get_scan_progress_value();
    let seconds = instant.elapsed().as_nanos() as f64 / 1e9f64;
    format!(
        "Read {} rows, {} in {:.3} sec., {} rows/sec., {}/sec.",
        progress.rows,
        convert_byte_size(progress.bytes as f64),
        seconds,
        convert_number_size((progress.rows as f64) / (seconds as f64)),
        convert_byte_size((progress.bytes as f64) / (seconds as f64)),
    )
}
