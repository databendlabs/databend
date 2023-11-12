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

use common_base::base::tokio;
use common_exception::Result;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::FuseTable;
use databend_query::test_kits::table_test_fixture::append_variant_sample_data;
use databend_query::test_kits::table_test_fixture::TestFixture;
use enterprise_query::storages::fuse::operations::virtual_columns::do_refresh_virtual_column;
use storages_common_cache::LoadParams;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_virtual_column() -> Result<()> {
    let fixture = TestFixture::new().await?;
    fixture
        .default_session()
        .get_settings()
        .set_retention_period(0)?;
    fixture.create_variant_table().await?;

    let number_of_block = 2;
    append_variant_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let virtual_columns = vec!["v:a".to_string(), "v:b".to_string()];
    let table_ctx = fixture.new_query_ctx().await?;
    do_refresh_virtual_column(fuse_table, table_ctx, virtual_columns).await?;

    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = snapshot_opt.unwrap();

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
            assert!(dal.is_exist(&virtual_loc).await?);
        }
    }

    Ok(())
}
