// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;

use crate::error::FuseQueryResult;

pub type SendableDataBlockStream = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = FuseQueryResult<DataBlock>> + Sync + Send>,
>;
