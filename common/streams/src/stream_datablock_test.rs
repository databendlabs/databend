// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datablocks::*;
use common_datavalues::*;
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
