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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_fuse::operations::AppendOperationLogEntry;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use databend_query::api::PrecommitBlock;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_precommit_ser_and_deser() -> Result<()> {
    let block_meta = BlockMeta::new(
        1,
        2,
        3,
        HashMap::new(),
        HashMap::new(),
        None,
        ("_b/1.json".to_string(), 1),
        None,
        4,
    );
    let segment_info = SegmentInfo::new(vec![Arc::new(block_meta)], Statistics::default());
    let log_entry = AppendOperationLogEntry::new("/_sg/1.json".to_string(), Arc::new(segment_info));
    let precommit_block = DataBlock::try_from(log_entry)?;
    let test_precommit = PrecommitBlock(precommit_block);

    let mut bytes = vec![];
    PrecommitBlock::write(test_precommit.clone(), &mut bytes)?;
    let mut read = bytes.as_slice();
    assert_eq!(test_precommit, PrecommitBlock::read(&mut read)?);
    Ok(())
}
