// Copyright 2020 Datafuse Labs.
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

use common_datablocks::*;
use common_datavalues::prelude::*;
use common_runtime::tokio;
use futures::stream::StreamExt;

use crate::*;

#[tokio::test]
async fn test_datablock_stream() {
    let mut s1 = DataBlockStream::create(Arc::new(DataSchema::empty()), None, vec![
        DataBlock::empty(),
        DataBlock::empty(),
        DataBlock::empty(),
    ]);
    while let Some(_) = s1.next().await {}
}
