//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_base::base::tokio;
use common_cache::Cache;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::statistics::gen_columns_statistics;
use common_storages_fuse::FuseStorageFormat;
use opendal::Operator;
use storages_common_cache::InMemoryCacheBuilder;
use storages_common_cache::InMemoryItemCacheHolder;
use storages_common_index::BloomIndexMeta;
use sysinfo::get_current_pid;
use sysinfo::ProcessExt;
use sysinfo::System;
use sysinfo::SystemExt;
use uuid::Uuid;

use crate::storages::fuse::block_writer::BlockWriter;

// NOTE:
//
// usage of memory is observed at *process* level, please do not combine them into
// one test function.
//
// by default, these cases are ignored (in CI).
//
// please run the following two cases individually (in different process)

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_index_meta_cache_size_file_meta_data() -> common_exception::Result<()> {
    let thrift_file_meta = setup().await?;

    let cache_number = 300_000;

    let meta: FileMetaData = FileMetaData::try_from_thrift(thrift_file_meta)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = "FileMetaData";

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<FileMetaData>(cache_number as u64);

    populate_cache(&cache, meta, cache_number);
    show_memory_usage(scenario, base_memory_usage, cache_number);

    drop(cache);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_index_meta_cache_size_bloom_meta() -> common_exception::Result<()> {
    let thrift_file_meta = setup().await?;

    let cache_number = 300_000;

    let bloom_index_meta = BloomIndexMeta::try_from(thrift_file_meta)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();

    let scenario = "BloomIndexMeta(mini)";
    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<BloomIndexMeta>(cache_number as u64);
    populate_cache(&cache, bloom_index_meta, cache_number);
    show_memory_usage("BloomIndexMeta(Mini)", base_memory_usage, cache_number);

    drop(cache);

    Ok(())
}

fn populate_cache<T>(cache: &InMemoryItemCacheHolder<T>, item: T, num_cache: usize)
where T: Clone {
    let mut c = cache.write();
    for _ in 0..num_cache {
        let uuid = Uuid::new_v4();
        (*c).put(
            format!("{}", uuid.simple()),
            std::sync::Arc::new(item.clone()),
        );
    }
}

async fn setup() -> common_exception::Result<ThriftFileMetaData> {
    let fields = (0..23)
        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
        .collect::<Vec<_>>();

    let schema = TableSchemaRefExt::create(fields);

    let mut columns = vec![];
    for _ in 0..schema.fields().len() {
        // values do not matter
        let column = Int32Type::from_data(vec![1]);
        columns.push(column)
    }

    let block = DataBlock::new_from_columns(columns);
    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());
    let col_stats = gen_columns_statistics(&block, None, &schema)?;
    let block_writer = BlockWriter::new(&operator, &loc_generator);
    let (_block_meta, thrift_file_meta) = block_writer
        .write(FuseStorageFormat::Parquet, &schema, block, col_stats, None)
        .await?;

    Ok(thrift_file_meta.unwrap())
}

fn show_memory_usage(case: &str, base_memory_usage: u64, num_cache_items: usize) {
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    {
        let memory_after = process.memory();
        let delta = memory_after - base_memory_usage;
        let delta_gb = (delta as f64) / 1024.0 / 1024.0 / 1024.0;
        eprintln!(
            "
            cache item type : {},
            number of cached items {},
            mem usage(B):{:+},
            mem usage(GB){:+}
            ",
            case, num_cache_items, delta, delta_gb
        );
    }
}
