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

use std::sync::Arc;

use common_base::tokio;
use common_base::*;
use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_streams::*;
use futures::TryStreamExt;

#[tokio::test]
async fn test_progress_stream() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i64::to_data_type())]);

    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1i64, 2, 3])]);

    let input = DataBlockStream::create(Arc::new(DataSchema::empty()), None, vec![
        block.clone(),
        block.clone(),
        block,
    ]);

    let progress = Arc::new(Progress::create());
    let stream = ProgressStream::try_create(Box::pin(input), progress)?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+---+", "| a |", "+---+", "| 1 |", "| 1 |", "| 1 |", "| 2 |", "| 2 |", "| 2 |", "| 3 |",
        "| 3 |", "| 3 |", "+---+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
