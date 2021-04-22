// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_datablock_stream() {
    use std::sync::Arc;

    use common_datablocks::*;
    use common_datavalues::*;
    use futures::stream::StreamExt;

    use crate::*;

    let mut s1 = DataBlockStream::create(Arc::new(DataSchema::empty()), None, vec![
        DataBlock::empty(),
        DataBlock::empty(),
        DataBlock::empty(),
    ]);
    while let Some(_) = s1.next().await {}
}
