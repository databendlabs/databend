// Copyright 2022 Datafuse Labs.
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

use common_config::Config;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FileFormatOptionsExt;
use common_meta_types::FileFormatOptions;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use databend_query::GlobalServices;
use tokio_stream::StreamExt;

pub async fn query_local(conf: &Config) -> Result<()> {
    let mut conf = conf.clone();
    conf.storage.allow_insecure = true;
    let local_conf = conf.local.clone();
    GlobalServices::init(conf).await?;

    let sql = get_sql(local_conf.sql, local_conf.table);

    let session = SessionManager::instance()
        .create_session(SessionType::Local)
        .await?;
    let ctx = session.create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(&sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let mut stream = interpreter.execute(ctx.clone()).await?;
    let first_block = match stream.next().await {
        Some(block) => match block {
            Ok(block) => block,
            Err(err) => return Err(err),
        },
        None => return Err(ErrorCode::Internal("no data block return")),
    };
    let mut output_fmt = FileFormatOptionsExt::get_output_format_from_options(
        first_block.schema().clone(),
        FileFormatOptions::default(),
        session.get_settings().as_ref(),
    )?;

    let ret = output_fmt.serialize_block(&first_block)?;
    print!("{}", String::from_utf8_lossy(&ret));

    while let Some(block) = stream.next().await {
        match block {
            Ok(block) => {
                // println!("data: {:?}", block.columns());
                let ret = output_fmt.serialize_block(&block)?;
                print!("{}", String::from_utf8_lossy(&ret));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
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
