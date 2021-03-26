// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datablocks::DataBlock;
use crate::error::FuseQueryResult;

pub type SendableDataBlockStream = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = FuseQueryResult<DataBlock>> + Sync + Send>,
>;
