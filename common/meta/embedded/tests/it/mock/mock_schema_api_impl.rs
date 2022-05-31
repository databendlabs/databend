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

use common_base::base::tokio;
use common_meta_api::SchemaApiTestSuite;
use common_meta_embedded::MetaEmbedded;

#[cfg(feature = "create_with_drop_time")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_table_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    SchemaApiTestSuite {}
        .table_drop_out_of_retention_time_history(&mt)
        .await
}

#[cfg(feature = "create_with_drop_time")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    SchemaApiTestSuite {}
        .database_drop_out_of_retention_time_history(&mt)
        .await
}
