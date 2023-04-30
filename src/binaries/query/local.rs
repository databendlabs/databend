// Copyright 2021 Datafuse Labs
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

use std::time::Instant;

use comfy_table::Cell;
use comfy_table::Table;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use databend_query::GlobalServices;
use tokio_stream::StreamExt;

pub async fn query_local(conf: &InnerConfig) -> Result<()> {
    let mut conf = conf.clone();
    conf.storage.allow_insecure = true;
    let local_conf = conf.local.clone();
    GlobalServices::init(conf).await?;

    let now = Instant::now();

    let sql = get_sql(local_conf.sql, local_conf.table);

    let res: Result<_> = try {
        let session = SessionManager::instance()
            .create_session(SessionType::Local)
            .await?;
        let ctx = session.create_query_context().await?;
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(&sql).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.map(|v| v).collect::<Vec<_>>().await;

        let schema = interpreter.schema();
        print_blocks(schema, blocks.as_slice(), now)?;
    };

    res.map_err(|err| err.display_with_sql(&sql))
}

fn get_sql(sql: String, table_str: String) -> String {
    let tables = table_str
        .split(',')
        .map(|v| {
            let a = v.split('=').map(|v| v.to_string()).collect::<Vec<String>>();
            a
        })
        .collect::<Vec<Vec<String>>>();

    let mut ret = sql;
    for table in tables {
        if table.len() == 2 {
            ret = ret.replace(
                table.get(0).unwrap(),
                &format!("read_parquet('{}')", table.get(1).unwrap()),
            );
        }
    }
    ret
}

fn print_blocks(
    schema: DataSchemaRef,
    results: &[Result<DataBlock, ErrorCode>],
    now: Instant,
) -> Result<()> {
    if let Err(err) = print_table(schema, results, now) {
        println!("Error: {}", err);
    }
    Ok(())
}

fn print_table(
    schema: DataSchemaRef,
    results: &[Result<DataBlock, ErrorCode>],
    now: Instant,
) -> Result<()> {
    if !results.is_empty() {
        if let Err(err) = &results[0] {
            return Err(err.clone());
        }
    }
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    if results.is_empty() {
        println!("{}", table);
        print_timing_info(0, now);
        return Ok(());
    }

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    let mut row_count = 0;
    for batch in results {
        let batch = batch.as_ref().map_err(ErrorCode::from_std_error)?;

        row_count += batch.num_rows();
        for (i, entry) in batch.columns().iter().enumerate() {
            table.add_row(vec![
                i.to_string(),
                entry.data_type.to_string(),
                format!("{:?}", entry.value),
            ]);
        }
    }

    println!("{}", table);
    print_timing_info(row_count, now);

    Ok(())
}

fn print_timing_info(row_count: usize, now: Instant) {
    println!(
        "{} {} in set. Query took {:.3} seconds.",
        row_count,
        if row_count == 1 { "row" } else { "rows" },
        now.elapsed().as_secs_f64()
    );
}
