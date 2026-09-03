//  Copyright 2021 Datafuse Labs.
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

use chrono::Duration;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

pub fn new_empty_snapshot(schema: TableSchema) -> TableSnapshot {
    TableSnapshot::try_new(
        None,
        None,
        schema,
        Statistics::default(),
        vec![],
        None,
        None,
        TableMetaTimestamps::new(None, Duration::hours(1)),
    )
    .unwrap()
}

pub async fn do_insertions(fixture: &TestFixture) -> Result<()> {
    fixture.create_default_table().await?;
    // ingests 1 block, 1 segment, 1 snapshot
    append_sample_data(1, fixture).await?;
    // then, overwrite the table, new data set: 1 block, 1 segment, 1 snapshot
    append_sample_data_overwrite(1, true, fixture).await?;
    Ok(())
}
