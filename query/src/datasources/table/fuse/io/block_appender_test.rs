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
//

use std::sync::Arc;

use common_base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use tempfile::TempDir;

use crate::datasources::table::fuse::io::BlockAppender;
use crate::datasources::table::fuse::DEFAULT_CHUNK_BLOCK_NUM;

#[tokio::test]
async fn test_fuse_table_block_appender() {
    let tmp_dir = TempDir::new().unwrap();
    let local_fs = common_dal::Local::with_path(tmp_dir.path().to_owned());
    let local_fs = Arc::new(local_fs);
    // some blocks
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1, 2, 3])]);
    let block_stream = futures::stream::iter(vec![Ok(block)]);
    let r = BlockAppender::append_blocks(
        local_fs.clone(),
        Box::pin(block_stream),
        schema.as_ref(),
        DEFAULT_CHUNK_BLOCK_NUM,
    )
    .await;
    assert!(r.is_ok());

    // non blocks
    let block_stream = futures::stream::iter(vec![]);
    let r = BlockAppender::append_blocks(
        local_fs,
        Box::pin(block_stream),
        schema.as_ref(),
        DEFAULT_CHUNK_BLOCK_NUM,
    )
    .await;
    assert!(r.is_ok());
    assert!(r.unwrap().is_empty())
}
