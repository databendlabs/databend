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

use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::UInt8Column;
use common_exception::Result;
use databend_query::api::PrecommitBlock;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_precommit_ser_and_deser() -> Result<()> {
    let data_schema = DataSchema::new(vec![DataField::new("test", UInt8Type::new_impl())]);

    let test_precommit = PrecommitBlock(DataBlock::create(Arc::new(data_schema), vec![Arc::new(
        UInt8Column::new_from_vec(vec![1, 2, 3]),
    )]));

    let mut bytes = vec![];
    PrecommitBlock::write(test_precommit.clone(), &mut bytes)?;
    let mut read = bytes.as_slice();
    assert_eq!(test_precommit, PrecommitBlock::read(&mut read)?);
    Ok(())
}
