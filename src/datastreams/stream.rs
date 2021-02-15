// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datablocks::DataBlock;
use crate::error::FuseQueryResult;

pub type SendableDataBlockStream = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = FuseQueryResult<DataBlock>> + Sync + Send>,
>;
