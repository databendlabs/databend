// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_planners::CreateTablePlan;
use common_planners::TableEngineType;
use common_planners::TableOptions;
use common_runtime::tokio;
use fuse_query::configs::Config;
use fuse_query::interpreters::InterpreterFactory;
use fuse_query::optimizers::Optimizers;
use fuse_query::sessions::FuseQueryContext;
use fuse_query::sql::PlanParser;
use futures::TryStreamExt;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Benchmarks", about = "Datafuse NYCTaxi Benchmarks.")]
struct Opt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of threads for query execution
    #[structopt(short = "t", long = "threads", default_value = "0")]
    threads: usize,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,
}

// cargo run --release --bin nyctaxi --path /xx/xx.csv
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    let ctx = FuseQueryContext::try_create(Config::default())?;
    if opt.threads > 0 {
        ctx.set_max_threads(opt.threads as u64)?;
    }

    println!(
        "Running benchmarks with the following options: {:?}, max_threads [{:?}], block_size[{:?}]",
        opt,
        ctx.get_settings().get_max_threads()?,
        ctx.get_settings().get_max_block_size()?
    );

    // Create csv table.
    let data_source = ctx.get_datasource();
    let database = data_source.get_database("default")?;
    let mut options: TableOptions = HashMap::new();
    options.insert("has_header".to_string(), "true".to_string());
    options.insert("location".to_string(), format!("{:?}", opt.path));
    let create_table_plan = CreateTablePlan {
        if_not_exists: false,
        db: "default".to_string(),
        table: "nyctaxi".to_string(),
        schema: nyctaxi_schema(),
        engine: TableEngineType::Csv,
        options,
    };
    database.create_table(create_table_plan).await?;

    let mut queries = HashMap::new();
    queries.insert("fare_amt_by_passenger", "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM nyctaxi  GROUP BY passenger_count");

    for (name, sql) in &queries {
        println!("Executing '{}'", name);
        println!("Query '{}'", sql);
        for i in 0..opt.iterations {
            let start = Instant::now();
            let plan = PlanParser::create(ctx.clone()).build_from_sql(sql)?;
            let plan = Optimizers::create(ctx.clone()).optimize(&plan).await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan)?;
            let stream = executor.execute().await?;

            let _ = stream.try_collect::<Vec<_>>().await?;
            println!(
                "Query '{}' iteration {} took {} ms",
                name,
                i,
                start.elapsed().as_millis()
            );
        }
    }

    Ok(())
}

fn nyctaxi_schema() -> DataSchemaRef {
    DataSchemaRefExt::create(vec![
        DataField::new("VendorID", DataType::Utf8, true),
        DataField::new("tpep_pickup_datetime", DataType::Utf8, true),
        DataField::new("tpep_dropoff_datetime", DataType::Utf8, true),
        DataField::new("passenger_count", DataType::Int32, true),
        DataField::new("trip_distance", DataType::Utf8, true),
        DataField::new("RatecodeID", DataType::Utf8, true),
        DataField::new("store_and_fwd_flag", DataType::Utf8, true),
        DataField::new("PULocationID", DataType::Utf8, true),
        DataField::new("DOLocationID", DataType::Utf8, true),
        DataField::new("payment_type", DataType::Utf8, true),
        DataField::new("fare_amount", DataType::Float64, true),
        DataField::new("extra", DataType::Float64, true),
        DataField::new("mta_tax", DataType::Float64, true),
        DataField::new("tip_amount", DataType::Float64, true),
        DataField::new("tolls_amount", DataType::Float64, true),
        DataField::new("improvement_surcharge", DataType::Float64, true),
        DataField::new("total_amount", DataType::Float64, true),
    ])
}
