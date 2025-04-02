// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_query::storages::fuse::operations::virtual_columns::do_refresh_virtual_column;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_virtual_column() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let number_of_block = 2;
    append_variant_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let table_ctx = fixture.new_query_ctx().await?;

    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = snapshot_opt.unwrap();

    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let mut build_res = PipelineBuildResult::create();
    let segment_locs = Some(snapshot.segments.clone());
    do_refresh_virtual_column(
        table_ctx.clone(),
        fuse_table,
        segment_locs,
        &mut build_res.main_pipeline,
    )
    .await?;

    let settings = table_ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(table_ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
        table_ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
    }

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        let block_metas = segment_info.block_metas()?;
        for block_meta in block_metas {
            let virtual_loc =
                TableMetaLocationGenerator::gen_virtual_block_location(&block_meta.location.0);
            assert!(dal.exists(&virtual_loc).await?);

            let schema = match storage_format {
                FuseStorageFormat::Parquet => read_parquet_schema_async_rs(dal, &virtual_loc, None)
                    .await
                    .ok(),
                FuseStorageFormat::Native => {
                    BlockReader::async_read_native_schema(dal, &virtual_loc)
                        .await
                        .map(|(_, schema)| schema)
                }
            };
            assert!(schema.is_some());
            let schema = schema.unwrap();
            assert_eq!(schema.fields.len(), 3);
            assert_eq!(schema.fields[0].name(), "v['a']");
            assert_eq!(schema.fields[1].name(), "v[0]");
            assert_eq!(schema.fields[2].name(), "v['b']");
        }
    }

    Ok(())
}
